import itertools

from spyne import ServiceBase, Unicode, Iterable, rpc

from model.Tweet import Tweet


class ServiceTopKUser(ServiceBase):
    @rpc(Unicode, _returns=Iterable(Unicode))
    def getTopKUser(ctx, K):
        tweets = ctx.udc.session.query(Tweet).all()
        # group by user
        tweets = {k: list(v) for k, v in itertools.groupby(tweets, key=lambda x: x.author_id)}
        # sort tweets by count
        tweets = sorted(tweets.items(), key=lambda x: len(x[1]), reverse=True)
        # get top K users
        tweets = tweets[:int(K)]
        # yield results
        for item in tweets:
            yield f"{item[0]}: {len(item[1])} tweets by this user"

