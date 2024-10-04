from konlpy.tag import Okt
from rake_nltk import Rake
import nltk

nltk.download('punkt_tab')
# 한국어 정지 단어 리스트 예제 (직접 추가 필요)
stopwords = set(
    [
        "의",
        "가",
        "이",
        "은",
        "들",
        "는",
        "좀",
        "잘",
        "걍",
        "과",
        "도",
        "를",
        "로",
        "으로",
        "자",
        "에",
        "와",
        "한",
        "을",
        "하다",
    ]
)

# Rake 인스턴스 생성, 한국어 정지 단어 설정
rake = Rake(stopwords=stopwords)

# KoNLPy 형태소 분석기 설정
okt = Okt()

# 분석할 한국어 텍스트
#text = "오늘의 저녁을 피자로 결정 했는데 다른 메뉴를 추천을 해줘"
text = "내일 친구와 영화를 보려고 계획중인데 어떤 영화가 좋을지 알려줘"
# 텍스트를 형태소 단위로 분리
words = okt.morphs(text)

# 형태소 리스트를 문자열로 변환
text_for_rake = " ".join(words)

# Rake 알고리즘 적용
rake.extract_keywords_from_text(text_for_rake)

# 추출된 키워드와 점수 출력
keyword_phrases = rake.get_ranked_phrases_with_scores()
print(keyword_phrases)
