REASONS = {
    "BAD_COLUMNS": "字段数量不符", "EMPTY_VALUE": "必要值为空",
    "DUPLICATE_CONFLICT": "同一业务键取值冲突", "TRUNCATED": "尾记录不完整",
    "INVALID_VALUE": "值不符合定义", "STALE_VERSION": "提交版本已过期",
    "POSITION_CONFLICT": "接收位置冲突，分片偏移与已接收位置不一致",
    "SOURCE_NOT_FOUND": "来源未登记", "GENERATION_NOT_FOUND": "文件代次不存在",
    "GENERATION_CLOSED": "代次已明确结尾，拒绝再写入",
    "SEGMENT_CONFLICT": "分片编号或校验值冲突",
    "UNKNOWN_ACTION": "未知采集动作", "INVALID_ARGUMENT": "参数不合法",
}

def reason(code: str) -> str:
    return REASONS.get(code, "未登记原因")
