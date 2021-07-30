from textblob import TextBlob

from textblob import TextBlob

def get_polarity(text):
  text=TextBlob(text).sentiment
  return text.polarity