

# trata textos, coloca tudo em minusculo (menos a primeira letra), remove acentos e caracteres especiais, espaços consecutivos
import unicodedata
import re
def clean_text(text):
    if text is None:
        return None
    # Remove acentos
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    # Remove caracteres especiais
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    # Remove espaços consecutivos
    text = re.sub(r'\s+', ' ', text).strip()
    # Coloca em minusculo e capitaliza a primeira letra de cada palavra
    text = text.lower().title()
    return text
