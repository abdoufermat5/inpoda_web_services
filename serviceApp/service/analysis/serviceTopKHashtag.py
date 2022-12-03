import itertools

from spyne import ServiceBase, Unicode, Iterable, rpc

from model.HashTag import HashTag


class ServiceTopKHashTag(ServiceBase):
    @rpc(Unicode, _returns=Iterable(Unicode))
    def getTopKHashTag(ctx, K):
        # get all hashtags
        hashtags = ctx.udc.session.query(HashTag).all()
        # group by hashtag
        hashtags = {k: list(v) for k, v in itertools.groupby(hashtags,
                                                             key=lambda x: x.hashtag)}
        # sort hashtags by count
        hashtags = sorted(hashtags.items(), key=lambda x: len(x[1]), reverse=True)
        # get top K hashtags
        hashtags = hashtags[:int(K)]
        # yield results
        for hashtag in hashtags:
            yield f"{hashtags.index(hashtag)+1} -> " \
                  f"{hashtag[0]}: {len(hashtag[1])} tweets with this hashtag"

