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

from qwen_agent.tools.simple_doc_parser import df_to_md

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


def content_analysis(idx, companyName, companySignature):
    try:
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

def create_industry_csv():
    file_name = 'industry.csv'

    if os.path.exists(file_name):
        try:
            df = pd.read_csv(file_name, dtype=str)
            logger.info(f"行业分类库文件 {file_name} 已存在，总共 {len(df)} 行")
        except Exception as e:
            logger.warning(f"行业分类库文件 {file_name} 已存在，但读取失败: {e}")
        return

    columns = ["签名", "类别", "更新时间", "是否人审"]

    df = pd.DataFrame(columns=columns)
    df.to_csv(file_name, index=False, encoding='utf-8-sig')
    logger.info(f"行业分类库文件 {file_name} 已成功新建")

