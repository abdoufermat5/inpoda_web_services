import itertools

from spyne import ServiceBase, Unicode, Iterable, rpc

from model.Tweet import Tweet

from model.Topic import Topic


class ServiceTopKTopic(ServiceBase):
    @rpc(Unicode, _returns=Iterable(Unicode))
    def getTopKTopic(ctx, K):
        topics = ctx.udc.session.query(Topic).all()
        # group by topic
        topics = {k: list(v)
                  for k, v in
                  itertools.groupby(topics, key=lambda x: x.topic)}
        # sort topics by count
        topics = sorted(topics.items(), key=lambda x: len(x[1]), reverse=True)
        # get top K topics
        topics = topics[:int(K)]
        # yield results
        for item in topics:
            yield f"{item[0]}: {len(item[1])} tweets about this topic"

