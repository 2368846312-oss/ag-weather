import requests
import datetime
import os
import time  # 【修复1】补上缺失的模块
from zoneinfo import ZoneInfo  # 【修复2】正确时区（Python3.9+）

# ========== 基准配置（今日2026-05-20 校准完毕） ==========
BASE_DATE = datetime.date(2026, 5, 20)

# 四大编号序列
BASE_NUM = {
    "pcp": 5072,        # 历史降水/降水距平
    "tmax_anom": 3036,  # 历史最高温距平
    "fcst_pcp": 3071,   # 未来降水/降水距平
    "fcst_tmp": 3078    # 未来气温距平
}

# ===================== 区域简写 =====================
AREA_CODE = {
    "北美": "na",
    "巴西": "br",
    "阿根廷": "ar",
    "欧盟": "eu",
    "西亚": "wa",
    "中国": "cn",
    "东南亚": "se",
    "澳洲": "au",
    "印度": "in",
    "非洲": "af"
}

# 图片模板库（通用模板，替换区域缩写即可）
IMG_TPLS = [
    # 历史降水
    {"type": "pcp", "tpl": "https://www.worldagweather.com/pastwx/pastpcp_{}_7day_{}.png", "name": "过去7天降雨"},
    {"type": "pcp", "tpl": "https://www.worldagweather.com/pastwx/pastpcp_{}_14day_{}.png", "name": "过去14天降雨"},
    {"type": "pcp", "tpl": "https://www.worldagweather.com/pastwx/pastpcp_{}_30day_{}.png", "name": "过去30天降雨"},
    # 历史降水距平
    {"type": "pcp", "tpl": "https://www.worldagweather.com/pastwx/pastpcp_anom_{}_14day_{}.png", "name": "过去14天降雨距平"},
    {"type": "pcp", "tpl": "https://www.worldagweather.com/pastwx/pastpcp_anom_{}_30day_{}.png", "name": "过去30天降雨距平"},
    # 历史最高温距平
    {"type": "tmax_anom", "tpl": "https://www.worldagweather.com/pastwx/pasttmax_anom_{}_7day_{}.png", "name": "过去7天最高温距平"},
    {"type": "tmax_anom", "tpl": "https://www.worldagweather.com/pastwx/pasttmax_anom_{}_14day_{}.png", "name": "过去14天最高温距平"},
    # 未来降水
    {"type": "fcst_pcp", "tpl": "https://www.worldagweather.com/fcstwx/pcp_ens_day7_q50_{}_{}.png", "name": "未来1-7天降雨"},
    {"type": "fcst_pcp", "tpl": "https://www.worldagweather.com/fcstwx/pcp_ens_day8_q50_{}_{}.png", "name": "未来8-14天降雨"},
    {"type": "fcst_pcp", "tpl": "https://www.worldagweather.com/fcstwx/pcp_ens_anom_q50_{}_{}.png", "name": "未来14天降水距平"},
    # 未来气温距平
    {"type": "fcst_tmp", "tpl": "https://www.worldagweather.com/fcstwx/tmp_gefs_day7_{}_{}.png", "name": "未来1-7天气温距平"},
    {"type": "fcst_tmp", "tpl": "https://www.worldagweather.com/fcstwx/tmp_gefs_day8_{}_{}.png", "name": "未来8-14天气温距平"},
]

ROOT_DIR = "全球天气图集"

# ---------- 【修复3】正确北京时间，永不跨天错乱 ----------
def get_today_beijing():
    beijing_tz = ZoneInfo("Asia/Shanghai")
    beijing_now = datetime.datetime.now(beijing_tz)
    return beijing_now.date()

today_beijing = get_today_beijing()
day_off = (today_beijing - BASE_DATE).days

# 打印日志，方便排查
print("="*50)
print(f"基准日期: {BASE_DATE}")
print(f"北京时间: {today_beijing}")
print(f"偏移天数: {day_off}")
print(f"当前时间戳: {int(time.time()*1000)}")
print("="*50 + "\n")

success = 0
fail = 0
total = 0

# 遍历下载
for area_name, area_suffix in AREA_CODE.items():
    print(f"\n========== 开始下载【{area_name}】 ==========")
    for item in IMG_TPLS:
        total += 1
        num = BASE_NUM[item["type"]] + day_off
        
        # 【修复4】强制不缓存，每次都拿最新图
        url = item["tpl"].format(area_suffix, num) + f"?t={int(time.time()*1000)}"
        save_path = os.path.join(ROOT_DIR, area_name, f"{item['name']}.png")
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        try:
            # 【修复5】增加请求头，模拟浏览器，防止被拦截
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            res = requests.get(url, headers=headers, timeout=20)
            res.raise_for_status()

            with open(save_path, "wb") as f:
                f.write(res.content)

            print(f"✅ {item['name']} 下载完成")
            success += 1

        except requests.exceptions.RequestException as e:
            print(f"❌ {item['name']} 下载失败 | {str(e)[:50]}...")
            fail += 1

print("\n" + "="*50)
print(f"任务完成 | 总计：{total} 张 | 成功：{success} | 失败：{fail}")
print(f"文件保存路径：{os.path.abspath(ROOT_DIR)}")
print("="*50)
