from spyne import ServiceBase, Unicode, Iterable, rpc

from model.HashTag import HashTag
from model.Sentiment import Sentiment

from model.Tweet import Tweet
from sqlalchemy import func


class ServiceNegativeSentiment(ServiceBase):
    @rpc(Unicode, _returns=Iterable(Unicode))
    def getNegativeSentiment(ctx, K):
        # for each hashtag return associated sentiment and tweet
        hashtags = ctx.udc.session.query(HashTag).all()

        result = ctx.udc.session.query(HashTag.hashtag, func.count(HashTag.id)). \
            join(Tweet, HashTag.tweet_id == Tweet.id). \
            join(Sentiment, HashTag.tweet_id == Sentiment.tweet_id). \
            filter(Sentiment.sentiment == 'negative'). \
            group_by(HashTag.text). \
            order_by(func.count(HashTag.id).desc()). \
            limit(5).all()
        print("------------------ SERVICE NEGATIVE SENTIMENT ------------------")
        print(result)
        print("---------------------------------------------------------------")

        for hashtag in result:
            yield f"{result.index(hashtag) + 1} -> " \
                  f"{hashtag[0]}: {hashtag[1]} tweets with this hashtag"



