from spyne import ServiceBase, Unicode, Iterable, rpc

from model.HashTag import HashTag
from model.Tweet import Tweet


class ServiceHashTagPublish(ServiceBase):
    @rpc(Unicode, _returns=Iterable(Unicode))
    def publishByHashTag(ctx, hashtag):
        hashtag = hashtag.lower()
        hashtags = ctx.udc.session.query(HashTag).filter_by(hashtag=hashtag).all()
        yield f"Found {len(hashtags)} tweets with hashtag {hashtag}"
        for h in hashtags:
            # get tweet from database
            print(h)
            tweet = ctx.udc.session.query(Tweet).filter_by(id=h.tweet_id).one()
            yield "--->" + tweet.text
