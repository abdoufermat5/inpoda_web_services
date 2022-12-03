from spyne import ServiceBase, srpc, String, UnsignedInteger, UnsignedInteger32, rpc, Unicode

from model.Tweet import Tweet

from model.Topic import Topic


class ServiceTopicIdentification(ServiceBase):

    @rpc(Unicode, _returns=String)
    def identifyTopic(ctx, tweet_id):
        # get tweet from database
        tweet = ctx.udc.session.query(Tweet).filter_by(id=tweet_id).one()
        topics = tweet.topic.split("<->")
        # save topics in Topic table
        for topic in topics:
            if ctx.udc.session.query(Topic).filter_by(topic=topic.strip().lower(), tweet_id=tweet_id).count() == 0:
                obj = Topic(topic=topic.strip().lower(), tweet_id=tweet_id)
                ctx.udc.session.add(obj)
                ctx.udc.session.flush()
        return f"TOPICS of this tweet: {tweet.topic}"
