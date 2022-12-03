from spyne import rpc, Mandatory as M, Service, ResourceNotFoundError, UnsignedInteger32, Iterable, String

from spyne.util import memoize


@memoize
def TweetCrudService(T, T_name):
    class CrudService(Service):
        @rpc(M(UnsignedInteger32, _returns=T))
        def getTweet(ctx, obj_id):
            return ctx.udc.session.query(T).filter_by(pk=obj_id).one()

        @rpc(T, _returns=String)
        def putTweet(ctx, obj):
            if obj.pk is None:
                # check if tweet already exists
                if ctx.udc.session.query(T).filter_by(id=obj.id).count() == 0:
                    ctx.udc.session.add(obj)
                    ctx.udc.session.flush()
                else:
                    return "Tweet already exists"

            else:
                if ctx.udc.session.query(T).get(obj.pk) is None:
                    raise ResourceNotFoundError('%s.id=%d' % (T_name, obj.pk))

                else:
                    ctx.udc.session.merge(obj)

            return f"Tweet {ctx.udc.session.query(T).get(obj.pk).id} successfully saved"

        @rpc(M(UnsignedInteger32))
        def delTweet(ctx, obj_id):
            count = ctx.udc.session.query(T).filter_by(pk=obj_id).count()
            if count == 0:
                raise ResourceNotFoundError(obj_id)

            ctx.udc.session.query(T).filter_by(pk=obj_id).delete()

        @rpc(_returns=Iterable(T))
        def getAllTweets(ctx):
            # get all tweets
            return ctx.udc.session.query(T).all()

    return CrudService
