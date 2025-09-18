import json
import base64
import hashlib
import math
import uuid
import time
import os
import glob
import gc
import pandas as pd
import logging
import requests
import shutil
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta

# 类别表示

category_dict = {
    "零售": "Sell",
    "教育培训": "Education",
    "软件科技": "Software",
    "招聘服务": "Recruitment",
    "金融": "Finance",
    "医疗健康": "Healthcare",
    "餐饮": "Catering",
    "旅游": "Tourism",
    "交通出行": "Transportation",
    "物流运输": "Logistics",
    "房地产": "House",
    "游戏": "Gaming",
    "媒体文化": "Media",
    "生活服务": "Lifestyle",
    "实业制造": "Manufacturing",
    "电信": "Telecommunications",
    "股票证券": "Securities",
    "其他行业": "Other_Industries"
}


# 日志部分

LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, 'industry_category.log')

logger = logging.getLogger('Industry_Category_Logger')
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(
    log_file, when='midnight', interval=1, backupCount=0, encoding='utf-8'
)
handler.suffix = "%Y%m%d.log"

formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
handler.setFormatter(formatter)

if not logger.hasHandlers():
    logger.addHandler(handler)


def clean_old_logs(log_dir=LOG_DIR, keep_days=30):
    threshold_date = datetime.now() - timedelta(days=keep_days)
    for file in os.listdir(log_dir):
        if file.startswith("industry_category.log.") and file.endswith(".log"):
            date_str = file.replace("industry_category.log.", "").replace(".log", "")
            try:
                file_date = datetime.strptime(date_str, "%Y%m%d")
                if file_date < threshold_date:
                    file_path = os.path.join(log_dir, file)
                    os.remove(file_path)
                    logger.info(f"已删除旧日志: {file_path}")
            except Exception as e:
                logger.error(f"删除日志文件出错: {file_path} -> {e}")


# 模型请求部分

api_url = 'http://10.253.123.46:29060/groupEh3iaK49/hlw5gsafe/zspt/signature-based-industry/text/ernie/v1/request'
app_id = 'rtaqcpzy'
app_key = 'b93ccd185fa386450708cce4b06c1732'


def getUUID():
    return "".join(str(uuid.uuid4()).split("-"))


def make_xServerParam():
    uuid = getUUID()
    appName = api_url.split("/")[3]
    for i in range(24 - len(appName)):
        appName += "0"
    capabilityname = appName
    csid = app_id + capabilityname + uuid
    tmp_xServerParam = {
        "appid": app_id,
        "csid": csid
    }
    xServerParam = str(base64.b64encode(json.dumps(tmp_xServerParam).encode("utf-8")), encoding="utf8")
    return xServerParam


def get_xCurTime():
    return str(math.floor(time.time()))


def get_xCheckSum(xCurTime, xServerParam):
    return hashlib.md5(bytes(app_key + xCurTime + xServerParam, encoding="utf8")).hexdigest()


def text_http_invoking(json_data, sid, sceneCode):
    xServerParam = make_xServerParam()
    xCurTime = get_xCurTime()
    xCheckSum = get_xCheckSum(xCurTime, xServerParam)
    headers = {
        "appKey": app_key,
        "Content-Type": "application/json",
        "sid": sid,
        "sceneCode": sceneCode,
        "X-Server-Param": xServerParam,
        "Authorization": "Bearer A_yk-X-msLPbYAe5Wr_-BGg5fD7Dq_wTMs7B92niMZs",
        "X-CurTime": xCurTime,
        "X-CheckSum": xCheckSum

    }
    resp = requests.post(url=api_url, headers=headers, json=json_data, verify=False)
    return resp.json()


def content_analysis(idx, companyName, companySignature, industry_dict):
    try:
        if industry_dict is not None and companySignature in industry_dict:
            logger.info(f"第 {idx} 条签名: {companySignature} 命中签名库，跳过")
            return "跳过"

        json_data = {"companyName": companyName, "companySignature": companySignature}
        msg_id = f"hesihang-{idx}"
        scene_code = "84"
        resp = text_http_invoking(json_data, sid=msg_id, sceneCode=scene_code)
        # resp = {'state': 'OK', 'body': {'filterType': 4, 'riskLevel': 3, 'label': '交通出行', 'percent': 1.0}}
        model_result = resp.get("body", {}).get("label")
        logger.info(f"第 {idx} 条，签名内容: {companySignature}，处理结果： {model_result}")
        return model_result
    except Exception as e:
        return {"error": f"响应过程中发生异常: {str(e)}"}


# 前置库管理部分

def open_industry_file(industry_file):
    if os.path.exists(industry_file):
        try:
            df = pd.read_csv(industry_file, dtype=str)
            logger.info(f"行业分类库文件 {industry_file} 已存在，总共 {len(df)} 行")
        except Exception as e:
            logger.warning(f"行业分类库文件 {industry_file} 已存在，但读取失败: {e}")
        return

    columns = ["签名", "类别", "更新时间", "是否人审"]

    df = pd.DataFrame(columns=columns)
    df.to_csv(industry_file, index=False, encoding='utf-8-sig')
    logger.info(f"行业分类库文件 {industry_file} 已成功新建")


def load_handled_review_files(record_file='industry_rs_done_dates.txt'):
    if os.path.exists(record_file):
        with open(record_file, 'r') as f:
            return set(line.strip() for line in f)
    return set()


def save_handled_review_file(filename, record_file='industry_rs_done_dates.txt'):
    with open(record_file, 'a') as f:
        f.write(filename + '\n')


def append_done_to_csv(done_local_path, industry_file):
    if not os.path.exists(done_local_path):
        logger.warning(f"人审结果文件 {done_local_path} 不存在，跳过更新")
        return

    if not os.path.exists(industry_file):
        logger.warning(f"前端库文件 {industry_file} 不存在，跳过更新")
        return

    try:
        df_industry = pd.read_csv(industry_file, dtype=str)
        df_done = pd.read_excel(done_local_path, dtype=str) if done_local_path.endswith(".xlsx") else pd.read_csv(done_local_path, dtype=str)
    except Exception as e:
        logger.error(f"读取文件失败: {e}")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updated_signs = set()

    for _, row in df_done.iterrows():
        sign = row.get("签名")
        correct_flag = row.get("是否正确")
        correct_category = row.get("正确类别")

        if sign not in df_industry["签名"].values:
            continue

        idx = df_industry[df_industry["签名"] == sign].index[0]

        if correct_flag == "是":
            df_industry.at[idx, "更新时间"] = now
            df_industry.at[idx, "是否人审"] = "是"
            updated_signs.add(sign)
        elif correct_flag == "否":
            df_industry.at[idx, "类别"] = correct_category
            df_industry.at[idx, "更新时间"] = now
            df_industry.at[idx, "是否人审"] = "是"
            updated_signs.add(sign)

    df_industry.to_csv(industry_file, index=False, encoding="utf-8-sig")

    added_count = len(updated_signs)
    logger.info(f"已使用人审结果 {done_local_path} 签名更新 {industry_file}，本次共更新签名 {added_count} 条")


def get_category_en(chinese_category):
    return category_dict.get(chinese_category, "Unknown Category")


def update_industry_file(current_date, industry_name, industry_file, done_remote_path, done_local_path):
    handled_review_files = load_handled_review_files()
    updated = False

    success_dates = []
    fail_dates = []

    category_file = get_category_en(industry_name)

    open_industry_file(industry_file)

    for delta in range(-30, 31):
        review_date = current_date - timedelta(days=delta)
        date_file = review_date.strftime('%Y%m%d')
        review_filename = f"{category_file}_result_{date_file}.xlsx"

        if review_filename in handled_review_files:
            continue

        remote_review_path = os.path.join(done_remote_path, review_filename)
        local_review_path = os.path.join(done_local_path, review_filename)

        if not os.path.exists(local_review_path):
            try:
                shutil.copy(remote_review_path, local_review_path)
                success_dates.append(date_file)
            except Exception:
                fail_dates.append(date_file)
                continue

        try:
            append_done_to_csv(
                done_local_path=local_review_path,
                industry_file=industry_file
            )
            save_handled_review_file(review_filename)
            updated = True
        except Exception as e:
            logger.error(f"处理人审文件 {review_filename} 失败: {e}")

    # 循环结束后统一打印日志
    if success_dates:
        logger.info(f"拷贝成功，共 {len(success_dates)} 个文件，日期如下：{'、'.join(success_dates)}")
    if fail_dates:
        logger.warning(f"拷贝失败，共 {len(fail_dates)} 个文件，日期如下：{'、'.join(fail_dates)}")

    if updated:
        logger.info("本次已成功更新签名库。")
    else:
        logger.info("近30天内无新增可用于更新签名库的人审文件。")


def load_industry_set(industry_file):

    industry_dict = {}

    if os.path.exists(industry_file):
        try:
            df = pd.read_csv(industry_file, dtype=str)
            if '签名' in df.columns and '类别' in df.columns:
                df = df.dropna(subset=['签名', '类别'])
                for _, row in df.iterrows():
                    signature = row['签名'].strip()
                    category = row['类别'].strip()
                    if signature:
                        industry_dict[signature] = category
                logger.info(f"成功加载行业签名 {len(industry_dict)} 个")
            else:
                logger.warning(f"行业分类库文件 {industry_file} 缺少必要列: {df.columns.tolist()}")
        except Exception as e:
            logger.error(f"读取行业分类库文件 {industry_file} 出错: {e}")
    else:
        logger.error(f"行业分类库文件 {industry_file} 不存在，跳过加载")

    return industry_dict

# 需要进行分类的源文件部分

def find_tsv_file_by_date(directory, date_str):
    pattern = os.path.join(directory, f'*{date_str}.tsv')
    files = glob.glob(pattern)
    return files[0] if files else None

def load_handled_dates(file_path='industry_model_done_dates.txt'):
    if not os.path.exists(file_path):
        return set()
    with open(file_path, 'r') as f:
        return set(line.strip() for line in f if line.strip())

def save_handled_date(date_str, file_path='industry_model_done_dates.txt'):
    with open(file_path, 'a') as f:
        f.write(date_str + '\n')


def open_tsv_data(file_dir, date_str):

    tsv_file = find_tsv_file_by_date(file_dir, date_str)

    if not tsv_file:
        logger.info(f"[{date_str}] 未找到TSV文件，1小时后重试...")
        time.sleep(3600)
        return None

    logger.info(f"找到TSV文件: {tsv_file}，正在打开")
    df = pd.read_csv(tsv_file, sep='\t', dtype=str)

    if '签名' not in df.columns:
        logger.warning('TSV文件中不包含“签名”列，跳过')
        save_handled_date(date_str)
        return None

    return df


# 处理及保存部分
def clean_old_excels(folder, industry_names, days=60):
    threshold_date = datetime.now() - timedelta(days=days)

    for cat in industry_names:
        prefix = f"{get_category_en(cat)}_result_"
        files = glob.glob(os.path.join(folder, f'{prefix}*.csv'))
        for file in files:
            try:
                basename = os.path.basename(file)
                date_part = basename.replace(prefix, '').replace('.csv', '')
                file_date = datetime.strptime(date_part, '%Y%m%d')
                if file_date < threshold_date:
                    os.remove(file)
                    logger.info(f"[{cat}] 已删除过期文件: {file}")
            except Exception as e:
                logger.error(f"[{cat}] 清理文件出错: {file} -> {e}")


def industry_model_work(df, date_str, output_base_dir, industry_names, industry_dict):

    # 存放结果的字典，按类别区分
    category_positive_results = {cat: [] for cat in industry_names}
    last_saved_count = {cat: 0 for cat in industry_names}

    # 输出文件路径（英文类别名）
    output_filenames = {
        cat: os.path.join(output_base_dir, f"{get_category_en(cat)}_result_{date_str}.csv")
        for cat in industry_names
    }

    # 提取需要处理的签名
    index_text_list = []
    for idx, row in df.iterrows():
        companyName = str(row['客户名称']).strip()
        companySignature = str(row['签名']).strip()
        if not companySignature:
            continue
        index_text_list.append((idx, companyName, companySignature))

    logger.info(f"总共需要处理：{len(index_text_list)} 个签名")

    for idx, companyName, companySignature in index_text_list:
        try:
            output = content_analysis(idx, companyName, companySignature, industry_dict)
        except Exception as e:
            output = {"error": f"处理失败: {str(e)}"}
            logger.error(f"签名 {companySignature} 处理异常: {e}")

        result = output.get("error", output) if isinstance(output, dict) else output

        # 只处理指定类别
        if result in category_positive_results:
            cat = result
            category_positive_results[cat].append((companySignature, result))

            # 每50条保存一次
            if len(category_positive_results[cat]) // 50 > last_saved_count[cat] // 50:
                try:
                    output_filename = output_filenames[cat]
                    if os.path.exists(output_filename):
                        df_existing = pd.read_csv(output_filename, dtype=str)
                    else:
                        df_existing = pd.DataFrame(columns=['签名', '分析结果'])
                    df_temp = pd.DataFrame(category_positive_results[cat], columns=['签名', '分析结果'])
                    df_combined = pd.concat([df_existing, df_temp], ignore_index=True)
                    df_combined.drop_duplicates(subset=['签名'], inplace=True)
                    df_combined.to_csv(output_filename, index=False, encoding='utf-8-sig')
                    logger.info(f"[{cat}] 中途保存：已写入 {len(df_combined)} 条记录到 {output_filename}")
                    last_saved_count[cat] = len(category_positive_results[cat])
                except Exception as e:
                    logger.error(f"[{cat}] 写入中途结果失败: {e}")

        elif result == '跳过':
            continue

        else:
            logger.info(f"签名 {companySignature} 返回结果 {result} 不在处理类别内，跳过")

    # 最终保存所有类别结果
    for cat, results_list in category_positive_results.items():
        try:
            if results_list:
                output_filename = output_filenames[cat]
                if os.path.exists(output_filename):
                    df_existing = pd.read_csv(output_filename, dtype=str)
                else:
                    df_existing = pd.DataFrame(columns=['签名', '分析结果'])
                df_temp = pd.DataFrame(results_list, columns=['签名', '分析结果'])
                df_combined = pd.concat([df_existing, df_temp], ignore_index=True)
                df_combined.drop_duplicates(subset=['签名'], inplace=True)
                df_combined.to_csv(output_filename, index=False, encoding='utf-8-sig')
                logger.info(f"[{cat}] 最终保存：已写入 {len(df_combined)} 条记录到 {output_filename}")

        except Exception as e:
            logger.error(f"[{cat}] 写入最终结果失败: {e}")

    for var in ['df', 'index_text_list', 'df_combined', 'df_temp']:
        if var in globals():
            del globals()[var]
    gc.collect()

    save_handled_date(date_str)

    clean_old_excels(folder=output_base_dir, industry_names=industry_names, days=60)


if __name__ == '__main__':
    industry_file = 'industry.csv' # 前置库文件
    industry_names = ['教育培训', '金融', '零售']  # 需要进行分类的类别名称
    tsv_data_path = '/data/rcsnas/zcz/5gmaap-quality-inspection/data/ChatbotList' # tsv文件路径
    model_result_path = '/home/hesihang/industry_category/output' # 模型分类结果目录
    done_remote_path = '/data/ftp/luoyang/sign/done' # 人审远程目录
    done_local_path = '/home/hesihang/industry_category/done' # 人审本地目录

    while True:
        start_date = datetime.now().date()
        handled_dates = load_handled_dates()
        logger.info(f"已处理日期: {handled_dates}")

        current_date = start_date
        while current_date.strftime('%Y%m%d') in handled_dates:
            current_date += timedelta(days=1)
        logger.info(f"当前处理日期: {current_date}")
        date_str = current_date.strftime('%Y%m%d')

        df = open_tsv_data(tsv_data_path, date_str)
        if df is None:
            continue

        for industry_name in industry_names:  # 更新前置库
            update_industry_file(current_date, industry_name, industry_file, done_remote_path, done_local_path)

        industry_signatures = load_industry_set(industry_file) # 加载前置库

        industry_model_work(df, date_str, model_result_path, industry_names, industry_signatures) # 模型处理

        clean_old_logs()

        logger.info(f"{date_str} 文件处理完毕，继续等待/处理下一天...")

        time.sleep(3600)