import re
import datetime
 
UTIL_CN_NUM = {
                u'零': 0,
                u'一': 1,
                u'二': 2,
                u'两': 2,
                u'三': 3,
                u'四': 4,
                u'五': 5,
                u'六': 6,
                u'七': 7,
                u'八': 8,
                u'九': 9,
                }
UTIL_CN_UNIT = {
                u'十': 10,
                u'百': 100,
                u'千': 1000,
                u'万': 10000,
                }
 
 
def cn2dig(src):
    if src == "":
        return None
    m = re.match("\d+", src)
    if m:
        return m.group(0)
    rsl = 0
    unit = 1
    for item in src[::-1]:
        if item in UTIL_CN_UNIT.keys():
            unit = UTIL_CN_UNIT[item]
        elif item in UTIL_CN_NUM.keys():
            num = UTIL_CN_NUM[item]
            rsl += num*unit
        else:
            return None
    if rsl < unit:
        rsl += unit
    return str(rsl)
 
def parse_datetime(msg):
    if msg is None or len(msg) == 0:
        return None
    m = re.match(ur"([0-9零一二两三四五六七八九十]+年)?([0-9一二两三四五六七八九十]+月)?([0-9一二两三四五六七八九十]+[号日])?([上下午晚早]+)?([0-9零一二两三四五六七八九十百]+[点:\.时])?([0-9零一二三四五六七八九十百]+分?)?([0-9零一二三四五六七八九十百]+秒)?", msg)
    if m.group(0) is not None:
        res = {
            "year": m.group(1),
            "month": m.group(2),
            "day": m.group(3),
            "hour": m.group(5) if m.group(5) is not None else '00',
            "minute": m.group(6) if m.group(6) is not None else '00',
            "second": m.group(7) if m.group(7) is not None else '00',
            # "microsecond": '00',
            }
        params = {}
        for name in res:
            if res[name] is not None and len(res[name]) != 0:
                params[name] = int(cn2dig(res[name][:-1]))
        target_date = datetime.datetime.today().replace(**params)
        is_pm = m.group(4)
        if is_pm is not None:
            if is_pm == u'下午' or is_pm == u'晚上':
                hour = target_date.time().hour
                if hour < 12:
                    target_date = target_date.replace(hour=hour+12)
        return target_date 
    else:
        return None
 
if __name__ == "__main__":
    print parse_datetime(None)
    print parse_datetime(u"两点30分")
    print parse_datetime(u"7点")
    print parse_datetime(u"五分")
    print parse_datetime(u"七点五分")
    print parse_datetime(u"七点零五分")
    print parse_datetime(u"9点04分")
    print parse_datetime(u"下午9点04分")
    print parse_datetime(u"6月三十日04分")
    print parse_datetime(u"1995年6月10号下午3点41分50秒")
