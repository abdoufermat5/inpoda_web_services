import re


def extractHashtagFromText(text):
    return re.findall(r"#(\w+)", text)


def predictTweetSentiment(text):
    import textblob
    from translate import Translator
    # translate text to english
    translator = Translator(to_lang="en", from_lang="fr")

    def getSubjectivity(text):
        return textblob.TextBlob(text).sentiment.subjectivity

        # Create a function to get the polarity

    def getPolarity(text):
        return textblob.TextBlob(text).sentiment.polarity

        # Create two new columns "Subjectivity" & "Polarity"
    text = translator.translate(text)
    print("TEXT --- ", text)
    subj = getSubjectivity(text)
    polarity = getPolarity(text)

    def getAnalysis(score):
        if score < 0:
            return "NEGATIVE"
        elif score == 0:
            return "NEUTRAL"
        else:
            return "POSITIVE"

    sentiment = getAnalysis(polarity)

    return sentiment, polarity, subj
