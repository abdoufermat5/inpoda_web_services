from spyne import ServiceBase, Unicode, Iterable, rpc

from model.Tweet import Tweet

from model.Topic import Topic


class ServiceTopicPublish(ServiceBase):
    @rpc(Unicode, _returns=Iterable(Unicode))
    def publishByTopic(ctx, topic):
        topic = topic.lower()
        topics = ctx.udc.session.query(Topic).filter_by(topic=topic).all()
        yield f"Found {len(topics)} tweets with topic {topic}"
        for t in topics:
            tweet = ctx.udc.session.query(Tweet).filter_by(id=t.tweet_id).one()
            yield "---> "+tweet.text
