import requests
import datetime
import os

# ==================== 路径配置 ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.join(BASE_DIR, "全球天气图集")

# ========== worldagweather 基准配置（今日2026-05-20 校准完毕） ==========
BASE_DATE = datetime.date(2026, 5, 20)
# 四大编号序列
BASE_NUM = {
    "pcp": 5072,        # 历史降水/降水距平
    "tmax_anom": 3036,  # 历史最高温距平
    "fcst_pcp": 3071,   # 未来降水/降水距平
    "fcst_tmp": 3078    # 未来气温距平
}

# ===================== worldagweather 区域简写 =====================
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

# worldagweather 图片模板库
IMG_TPLS = [
    # 历史降水
    {"type":"pcp","tpl":"https://www.worldagweather.com/pastwx/pastpcp_{}_7day_{}.png","name":"过去7天降雨"},
    {"type":"pcp","tpl":"https://www.worldagweather.com/pastwx/pastpcp_{}_14day_{}.png","name":"过去14天降雨"},
    {"type":"pcp","tpl":"https://www.worldagweather.com/pastwx/pastpcp_{}_30day_{}.png","name":"过去30天降雨"},
    # 历史降水距平
    {"type":"pcp","tpl":"https://www.worldagweather.com/pastwx/pastpcp_anom_{}_14day_{}.png","name":"过去14天降雨距平"},
    {"type":"pcp","tpl":"https://www.worldagweather.com/pastwx/pastpcp_anom_{}_30day_{}.png","name":"过去30天降雨距平"},
    # 历史最高温距平
    {"type":"tmax_anom","tpl":"https://www.worldagweather.com/pastwx/pasttmax_anom_{}_7day_{}.png","name":"过去7天最高温距平"},
    {"type":"tmax_anom","tpl":"https://www.worldagweather.com/pastwx/pasttmax_anom_{}_14day_{}.png","name":"过去14天最高温距平"},
    # 未来降水
    {"type":"fcst_pcp","tpl":"https://www.worldagweather.com/fcstwx/pcp_ens_day7_q50_{}_{}.png","name":"未来1-7天降雨"},
    {"type":"fcst_pcp","tpl":"https://www.worldagweather.com/fcstwx/pcp_ens_day8_q50_{}_{}.png","name":"未来8-14天降雨"},
    {"type":"fcst_pcp","tpl":"https://www.worldagweather.com/fcstwx/pcp_ens_anom_q50_{}_{}.png","name":"未来14天降水距平"},
    # 未来气温距平
    {"type":"fcst_tmp","tpl":"https://www.worldagweather.com/fcstwx/tmp_gefs_day7_{}_{}.png","name":"未来1-7天气温距平"},
    {"type":"fcst_tmp","tpl":"https://www.worldagweather.com/fcstwx/tmp_gefs_day8_{}_{}.png","name":"未来8-14天气温距平"},
]

# ==================== NMC 中央气象台配置 ====================
NMC_BASE = "https://image.nmc.cn/product"

PCP_HOURS = {24: "02400", 48: "04800", 72: "07200", 96: "09600",
             120: "12000", 144: "14400", 168: "16800"}
TMP_HOURS = {24: "02412", 48: "04812", 72: "07212", 96: "09612",
             120: "12012", 144: "14412", 168: "16812"}

# ==================== 辅助函数 ====================

def build_nmc_urls(date_obj):
    """根据日期构建 NMC 14 张图的 URL 列表"""
    y, m, d = date_obj.year, date_obj.month, date_obj.day
    ymd_str = f"{y}{m:02d}{d:02d}"
    urls = []

    for hour, code in PCP_HOURS.items():
        url = f"{NMC_BASE}/{y}/{m:02d}/{d:02d}/STFC/medium/SEVP_NMC_STFC_SFER_ER24_ACHN_L88_P9_{ymd_str}0000{code}.JPG"
        filename = f"降雨预报_{hour}h.jpg"
        urls.append((url, filename, f"降水预报_{hour}h"))

    for hour, code in TMP_HOURS.items():
        url = f"{NMC_BASE}/{y}/{m:02d}/{d:02d}/RFFC/medium/SEVP_NMC_RFFC_SNWFD_ETM_ACHN_L88_P9_{ymd_str}0800{code}.jpg"
        filename = f"最高气温预报_{hour}h.jpg"
        urls.append((url, filename, f"最高气温预报_{hour}h"))

    return urls


def download_nmc(save_dir, date_obj):
    """下载 NMC 中国天气图"""
    urls = build_nmc_urls(date_obj)
    success = 0
    fail = 0
    print(f"\n{'='*50}")
    print(f"【NMC 中央气象台 — 中国】日期: {date_obj}")
    print(f"{'='*50}")

    for url, filename, label in urls:
        save_path = os.path.join(save_dir, filename)
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            if "image" not in content_type and len(resp.content) < 5000:
                print(f"⚠️ {label}  → 可能未发布（{len(resp.content)} bytes）")
                fail += 1
                continue
            with open(save_path, "wb") as f:
                f.write(resp.content)
            print(f"✅ {label}  → {filename}")
            success += 1
        except requests.RequestException as e:
            print(f"❌ {label}  → {e}")
            fail += 1

    # 全部失败则尝试昨日回退
    if success == 0:
        yesterday = date_obj - datetime.timedelta(days=1)
        print(f"\n⚠️ 全失败，尝试昨日 ({yesterday}) ...")
        return download_nmc(save_dir, yesterday)

    return success, fail


# ==================== 主流程 ====================

if __name__ == "__main__":
    today = datetime.date.today()

    # ————— 第一步：worldagweather 全球区域下载 —————
    day_off = (today - BASE_DATE).days
    print(f"基准日期:{BASE_DATE}  今日:{today}  偏移天数:{day_off}")
    print(f"{'='*50}")
    print("【worldagweather — 全球区域】")
    print(f"{'='*50}")

    waw_success = 0
    waw_fail = 0
    waw_total = 0

    for area_name, area_suffix in AREA_CODE.items():
        print(f"\n--- {area_name} ---")
        for item in IMG_TPLS:
            waw_total += 1
            num = BASE_NUM[item["type"]] + day_off
            url = item["tpl"].format(area_suffix, num)
            save_path = os.path.join(ROOT_DIR, area_name, f"{item['name']}.png")
            os.makedirs(os.path.dirname(save_path), exist_ok=True)

            try:
                res = requests.get(url, timeout=15)
                res.raise_for_status()
                with open(save_path, "wb") as f:
                    f.write(res.content)
                print(f"✅ {item['name']}")
                waw_success += 1
            except:
                print(f"❌ {item['name']} 失败")
                waw_fail += 1

    # ————— 第二步：NMC 中国 14 张图 —————
    nmc_dir = os.path.join(ROOT_DIR, "中国")
    os.makedirs(nmc_dir, exist_ok=True)
    nmc_success, nmc_fail = download_nmc(nmc_dir, today)

    # ————— 汇总 —————
    total_success = waw_success + nmc_success
    total_fail = waw_fail + nmc_fail
    total_all = waw_total + 14

    print(f"\n{'='*60}")
    print(f"全部任务结束")
    print(f"  worldagweather: 成功 {waw_success} / 失败 {waw_fail} / 小计 {waw_total}")
    print(f"  NMC 中央气象台:  成功 {nmc_success} / 失败 {nmc_fail} / 小计 14")
    print(f"  总计: {total_success} / {total_fail} / {total_all}")
    print(f"  文件存放根目录: {os.path.abspath(ROOT_DIR)}")
