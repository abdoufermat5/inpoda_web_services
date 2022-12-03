from spyne import ServiceBase, srpc, String, UnsignedInteger32, Array, rpc, Unicode

from model.Tweet import Tweet
from model.Sentiment import Sentiment
from service.preprocessing.utils.utils import predictTweetSentiment


class ServiceSentimentAnalysis(ServiceBase):
    @rpc(Unicode, _returns=String)
    def predictSentiment(ctx, tweet_id):
        sentiment, polarity, subjectivity = predictTweetSentiment(
            ctx.udc.session.query(Tweet)
            .filter_by(id=tweet_id)
            .one().text)
        # save extracted hashtags in database
        obj = Sentiment(sentiment=sentiment.lower(),
                        tweet_id=tweet_id,
                        polarity=polarity,
                        subjectivity=subjectivity)
        # check if hashtag already exists
        if ctx.udc.session.query(Sentiment) \
                .filter_by(sentiment=sentiment,
                           tweet_id=tweet_id) \
                .count() == 0:
            ctx.udc.session.add(obj)
            ctx.udc.session.flush()

        return f"Sentiment of this tweet is {sentiment}"
