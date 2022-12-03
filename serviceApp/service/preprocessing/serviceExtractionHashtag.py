from spyne import ServiceBase, srpc, String, UnsignedInteger32, Array, rpc, Unicode

from model.Tweet import Tweet
from model.HashTag import HashTag

from service.preprocessing.utils.utils import extractHashtagFromText


class ServiceExtractionHashtag(ServiceBase):
    @rpc(Unicode, _returns=String)
    def extractTweetHashtag(ctx, tweet_id):
        extracted_hashtags = extractHashtagFromText(ctx.udc.session.query(Tweet)
                                                    .filter_by(id=tweet_id)
                                                    .one().text)
        # save extracted hashtags in database
        for hashtag in extracted_hashtags:
            obj = HashTag(hashtag=hashtag.lower(), tweet_id=tweet_id)
            # check if hashtag already exists
            if ctx.udc.session.query(HashTag).\
                    filter_by(hashtag=hashtag.lower(), tweet_id=tweet_id)\
                    .count() == 0:
                ctx.udc.session.add(obj)
                ctx.udc.session.flush()
        extracted_hashtags = ["#" + hashtag for hashtag in extracted_hashtags]
        if len(extracted_hashtags) == 0:
            extracted_hashtags = ["No hashtags found"]
        return "HASHTAG of this tweet:" + " ".join(extracted_hashtags)
