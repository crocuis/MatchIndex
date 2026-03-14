from __future__ import annotations

import json
import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYDEPS = ROOT / '.pydeps'
if str(PYDEPS) not in sys.path:
    sys.path.insert(0, str(PYDEPS))

from hangulize import hangulize  # type: ignore


LANG_BY_COUNTRY = {
    'ENG': 'eng',
    'SCO': 'eng',
    'WAL': 'eng',
    'NIR': 'eng',
    'GBR': 'eng',
    'FRA': 'fra',
    'FR': 'fra',
    'ESP': 'spa',
    'ES': 'spa',
    'DEU': 'deu',
    'DE': 'deu',
    'AUT': 'deu',
    'ITA': 'ita',
    'IT': 'ita',
    'NLD': 'nld',
    'BEL': 'nld',
    'POR': 'por',
    'CZE': 'ces',
    'SVK': 'slk',
    'HRV': 'hbs',
    'SRB': 'hbs',
    'BIH': 'hbs',
    'MNE': 'hbs',
    'SVN': 'slv',
    'POL': 'pol',
    'HUN': 'hun',
    'ROU': 'ron',
    'GRC': 'ell',
    'GRE': 'ell',
    'UKR': 'ukr',
    'TUR': 'tur',
    'EST': 'est',
    'LVA': 'lav',
    'LTU': 'lit',
    'MKD': 'mkd',
    'ISL': 'isl',
    'SWE': 'swe',
    'FIN': 'fin',
}

TOKEN_MAP = {
    'town': '타운',
    'united': '유나이티드',
    'city': '시티',
    'rangers': '레인저스',
    'rovers': '로버스',
    'wanderers': '원더러스',
    'county': '카운티',
    'athletic': '애슬레틱',
    'atletic': '아틀레틱',
    'atletico': '아틀레티코',
    'albion': '앨비언',
    'forest': '포리스트',
    'palace': '팰리스',
    'sporting': '스포르팅',
    'sport': '스포르트',
    'club': '클럽',
    'clube': '클루브',
    'deportivo': '데포르티보',
    'real': '레알',
    'olympique': '올랭피크',
    'olympiakos': '올림피아코스',
    'borussia': '보루시아',
    'eintracht': '아인트라흐트',
    'sociedad': '소시에다드',
    'betis': '베티스',
    'balompie': '발롬피에',
    'saint': '생',
    'st': '세인트',
    'sainte': '생트',
    'san': '산',
    'santa': '산타',
    'womens': '여자',
    'women': '여자',
    'woman': '여자',
    'girls': '여자',
    'ladies': '여자',
    'youth': '유스',
    'and': '앤드',
    'old': '올드',
    'boys': '보이스',
    'young': '영',
    'red': '레드',
    'bull': '불',
    'diamond': '다이아몬드',
    'diamonds': '다이아몬즈',
    'ashby': '애시비',
    'ashford': '애슈퍼드',
    'ashton': '애슈턴',
    'barnsley': '반슬리',
    'barrow': '배로',
    'boreham': '보어럼',
    'bootle': '부틀',
    'bournemouth': '본머스',
    'braintree': '브레인트리',
    'brentwood': '브렌트우드',
    'cleethorpes': '클리소프스',
    'eastbourne': '이스트본',
    'eastwood': '이스트우드',
    'ebbsfleet': '앱스플리트',
    'fleet': '플리트',
    'fleetwood': '플리트우드',
    'hashtag': '해시태그',
    'hornchurch': '혼처치',
    'leeds': '리즈',
    'metz': '메츠',
    'northwich': '노스위치',
    'northwood': '노스우드',
    'norwich': '노리치',
    'shakhtar': '샤흐타르',
    'donetsk': '도네츠크',
    'shrewsbury': '슈루즈버리',
    'strasbourg': '스트라스부르',
    'street': '스트리트',
    'totton': '토턴',
    'walsall': '월솔',
    'wimbledon': '윔블던',
    'whyteleafe': '화이트리프',
    'wood': '우드',
    'woodbridge': '우드브리지',
    'woodford': '우드퍼드',
    'york': '요크',
    'u17': 'U17',
    'u18': 'U18',
    'u19': 'U19',
    'u20': 'U20',
    'u21': 'U21',
    'u23': 'U23',
    'fc': 'FC',
    'afc': 'AFC',
    'ac': 'AC',
    'as': 'AS',
    'sc': 'SC',
    'rc': 'RC',
    'rcd': 'RCD',
    'cf': 'CF',
    'cfc': 'CFC',
    'bc': 'BC',
    'fk': 'FK',
    'sk': 'SK',
    'kv': 'KV',
    'ud': 'UD',
    'cd': 'CD',
    'ca': 'CA',
    'us': 'US',
    'ssc': 'SSC',
    'sv': 'SV',
    'fsv': 'FSV',
    'tsg': 'TSG',
    'vfb': 'VfB',
    'vfl': 'VfL',
    'bk': 'BK',
    'if': 'IF',
    'ki': 'KI',
    'hjk': 'HJK',
    'ol': 'OL',
    'wfc': 'WFC',
    'fcw': 'FCW',
    'lfc': 'LFC',
    'wnt': 'WNT',
    'psv': 'PSV',
    'paok': 'PAOK',
    'fcsb': 'FCSB',
    'aek': 'AEK',
    'hnk': 'HNK',
    'lask': 'LASK',
    'sco': 'SCO',
    'osc': 'OSC',
}

NAME_OVERRIDES = {
    'aberdeen': ('애버딘', '애버딘'),
    'aek-athens-fc': ('AEK 아테네', 'AEK 아테네'),
    'afc-wimbledon': ('AFC 윔블던', '윔블던'),
    'afc-whyteleafe': ('AFC 화이트리프', '화이트리프'),
    'afc-hornchurch': ('AFC 혼처치', '혼처치'),
    'afc-rushden-and-diamonds': ('AFC 러시든 앤드 다이아몬즈', '러시든 앤드 다이아몬즈'),
    'apm-metz': ('APM 메츠', 'APM 메츠'),
    'antwerp': ('안트베르펜', '안트베르펜'),
    'aris': ('아리스', '아리스'),
    'atletico-madrid': ('아틀레티코 마드리드', '아틀레티코'),
    'andover-new-street': ('앤도버 뉴 스트리트', '앤도버 뉴 스트리트'),
    'barrow': ('배로', '배로'),
    'ballkani': ('발카니', '발카니'),
    'bate-borisov': ('바테 보리소프', '바테'),
    'bk-hacken': ('BK 헤켄', '헤켄'),
    'breidablik': ('브레이다블리크', '브레이다블리크'),
    'buducnost-podgorica': ('부두치노스트 포드고리차', '부두치노스트'),
    'celtic': ('셀틱', '셀틱'),
    'cukaricki': ('추카리치키', '추카리치키'),
    'dec-ic': ('데치치', '데치치'),
    'dinamo-batumi': ('디나모 바투미', '디나모 바투미'),
    'dinamo-tbilisi': ('디나모 트빌리시', '디나모 트빌리시'),
    'dnipro-1': ('드니프로-1', '드니프로-1'),
    'egnatia-rrogozhine': ('에그나티아 로고지너', '에그나티아'),
    'farul-constanta': ('파룰 콘스탄차', '파룰'),
    'fc-astana': ('FC 아스타나', '아스타나'),
    'fc-copenhagen': ('FC 코펜하겐', '코펜하겐'),
    'fc-differdange-03': ('FC 디페르당주 03', '디페르당주'),
    'fc-equeurdreville': ('FC 에케르드르빌', '에케르드르빌'),
    'fc-red-bull-salzburg-austria': ('FC 레드불 잘츠부르크', '레드불 잘츠부르크'),
    'fc-urartu': ('FC 우라르투', '우라르투'),
    'fleet-town': ('플리트 타운', '플리트 타운'),
    'fleetwood-town': ('플리트우드 타운', '플리트우드 타운'),
    'fk-shakhtar-donetsk-ukraine': ('FK 샤흐타르 도네츠크', '샤흐타르 도네츠크'),
    'fk-zalgiris-vilnius': ('FK 잘기리스 빌뉴스', '잘기리스'),
    'flora-tallinn': ('플로라 탈린', '플로라 탈린'),
    'genk': ('헹크', '헹크'),
    'hamrun-spartans': ('함룬 스파르탄스', '함룬'),
    'hjk-helsinki': ('HJK 헬싱키', '헬싱키'),
    'larne': ('라른', '라른'),
    'maccabi-haifa': ('마카비 하이파', '마카비 하이파'),
    'northwich-victoria': ('노스위치 빅토리아', '노스위치 빅토리아'),
    'norwich': ('노리치', '노리치'),
    'norwich-united': ('노리치 유나이티드', '노리치 유나이티드'),
    'olimpija-ljubljana': ('올림피야 류블랴나', '올림피야'),
    'ordabasy': ('오르다바시', '오르다바시'),
    'partizani': ('파르티자니', '파르티자니'),
    'pyunik-yerevan': ('퓌니크 예레반', '퓌니크'),
    'rako-w-cze-stochowa': ('라쿠프 쳉스토호바', '라쿠프'),
    'red-bull-salzburg': ('레드불 잘츠부르크', '잘츠부르크'),
    'royal-wootton': ('로열 우턴', '로열 우턴'),
    'street': ('스트리트', '스트리트'),
    'strasbourg': ('스트라스부르', '스트라스부르'),
    'struga': ('스트루가', '스트루가'),
    'swift-hesperange': ('스위프트 에스페랑주', '스위프트'),
    'tre-penne': ('트레 펜네', '트레 펜네'),
    'valmiera-bss': ('발미에라/BSS', '발미에라'),
    'vikingur-reykjavik': ('비킹귀르 레이캬비크', '비킹귀르'),
    'virtus': ('비르투스', '비르투스'),
    'zorya-luhansk': ('조랴 루한스크', '조랴'),
    'zrinjski': ('즈린스키', '즈린스키'),
    'wimbledon': ('윔블던', '윔블던'),
    'woodbridge-town': ('우드브리지 타운', '우드브리지 타운'),
    'woodford-town': ('우드퍼드 타운', '우드퍼드 타운'),
    'york': ('요크', '요크'),
    'barnsley': ('반슬리', '반슬리'),
    'bournemouth': ('본머스', '본머스'),
}

TRAILING_GENERIC_MARKERS = {
    'FC', 'CF', 'SC', 'AC', 'AFC', 'CFC', 'BC', 'KV', 'OSC', 'SCO', 'WFC', 'FCW', 'LFC',
}


def normalize_ascii(value: str) -> str:
    return unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')


def rewrite_english_token(token: str) -> str:
    rewritten = token.lower()
    rewritten = rewritten.replace('sley', 'sli')
    rewritten = rewritten.replace('bury', 'beri')
    rewritten = rewritten.replace('borough', 'boro')
    rewritten = rewritten.replace('mouth', 'muth')
    rewritten = rewritten.replace('leigh', 'li')
    rewritten = rewritten.replace('eigh', 'ei')
    rewritten = rewritten.replace('ough', 'off')
    rewritten = rewritten.replace('augh', 'a')
    rewritten = rewritten.replace('wh', 'w')
    return rewritten


def transliterate_with_hangulize(token: str, language: str) -> str:
    if language == 'eng':
        source = rewrite_english_token(normalize_ascii(token))
        target = 'lat'
    elif language == 'fra':
        source = normalize_ascii(token).lower()
        target = 'lat'
    else:
        source = token
        target = language

    try:
        result = hangulize(source, target)
    except Exception:
        fallback = normalize_ascii(token).lower()
        return hangulize(fallback, 'lat')

    if re.search(r'[A-Za-z]{2,}', result):
        fallback = normalize_ascii(token).lower()
        return hangulize(fallback, 'lat')

    return result


def is_all_caps_token(token: str) -> bool:
    letters = re.sub(r'[^A-Za-z]', '', token)
    return bool(letters) and letters == letters.upper()


def transliterate_token(token: str, language: str) -> str:
    stripped = token.strip()
    if not stripped:
        return ''

    cleaned = stripped.strip("()[]{}.,'`\"")
    if not cleaned:
        return ''

    lowered = normalize_ascii(cleaned).lower()

    if lowered in TOKEN_MAP:
        return TOKEN_MAP[lowered]

    if re.fullmatch(r'U\d{2}', cleaned.upper()):
        return cleaned.upper()

    if re.fullmatch(r'\d{2,4}', cleaned):
        return cleaned

    if is_all_caps_token(cleaned) and len(cleaned) <= 5:
        return cleaned

    return transliterate_with_hangulize(cleaned, language)


def cleanup_spacing(value: str) -> str:
    value = value.replace(' / ', '/')
    value = re.sub(r'\s+', ' ', value)
    return value.strip()


def derive_short_name(name: str) -> str:
    tokens = name.split()
    while len(tokens) > 1 and tokens[-1] in TRAILING_GENERIC_MARKERS:
        tokens.pop()

    if len(tokens) >= 2 and tokens[0] in {'FC', 'AFC', 'AC', 'AS', 'SC', 'RC', 'RCD', 'FK', 'SK', 'BK', 'IF', 'HJK', 'HNK'}:
        return ' '.join(tokens[1:])

    if len(tokens) >= 2 and tokens[0] in {'1.', '1', 'VfB', 'VfL', 'FSV', 'TSG', 'SV', 'SSC', 'US', 'UD', 'CD', 'CA'}:
        return ' '.join(tokens[1:])

    if len(tokens) >= 3 and tokens[0] in {'스타드', '올랭피크', '라싱', '디나모', '레드불'}:
        return ' '.join(tokens[1:])

    return ' '.join(tokens)


def generate_name(row: dict[str, object]) -> tuple[str, str]:
    slug = str(row['slug'])
    if slug in NAME_OVERRIDES:
        return NAME_OVERRIDES[slug]

    name = str(row['enName'])
    country_code = str(row['countryCode']).strip().upper()
    language = LANG_BY_COUNTRY.get(country_code, 'lat')

    expanded = name.replace('&', ' and ').replace('/', ' / ')
    tokens = [token for token in re.split(r'\s+', expanded) if token]
    localized_tokens = [transliterate_token(token, language) for token in tokens]
    localized_name = cleanup_spacing(' '.join(token for token in localized_tokens if token))
    short_name = cleanup_spacing(derive_short_name(localized_name))
    return localized_name, short_name


def main() -> None:
    input_path = ROOT / '.sisyphus' / 'team-ko-review' / 'latest-team-ko-missing.full.json'
    output_path = ROOT / 'scripts' / 'ko-team-names.generated.json'

    rows = json.loads(input_path.read_text())
    output: dict[str, dict[str, str]] = {}
    for row in rows:
        name, short_name = generate_name(row)
        output[row['slug']] = {
            'name': name,
            'shortName': short_name,
        }

    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'count': len(output), 'outputPath': str(output_path)}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
