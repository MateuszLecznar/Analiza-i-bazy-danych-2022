from textblob import TextBlob
def extract_sentiment(text):
    text = TextBlob(text)

    return text.sentiment.polarity


def text_contain_word(word: str, text: str):
    return word in text

def selection_sort_asc(tab):
    size=len(tab)
    for step in range(size):
        min = step

        for i in range(step + 1, size):
            if tab[i] < tab[min]:
                min = i
        
        (tab[step], tab[min]) = (tab[min], tab[step])
    return tab

def selection_sort_desc(tab):
    size=len(tab)
    for step in range(size):
        min = step

        for i in range(step + 1, size):
            if tab[i] > tab[min]:
                min = i
        
        (tab[step], tab[min]) = (tab[min], tab[step])
    return tab
