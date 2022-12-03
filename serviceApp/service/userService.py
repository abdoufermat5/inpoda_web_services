from spyne import rpc, Mandatory as M, Service, ResourceNotFoundError, UnsignedInteger32, Iterable

from spyne.util import memoize


@memoize
def UserCrudService(T, T_name):
    class CrudService(Service):
        @rpc(M(UnsignedInteger32, _returns=T))
        def getUser(ctx, obj_id):
            return ctx.udc.session.query(T).filter_by(id=obj_id).one()

        @rpc(T, _returns=UnsignedInteger32)
        def putUser(ctx, obj):
            if obj.id is None:
                ctx.udc.session.add(obj)
                ctx.udc.session.flush()

            else:
                if ctx.udc.session.query(T).get(obj.id) is None:
                    raise ResourceNotFoundError('%s.id=%d' % (T_name, obj.id))

                else:
                    ctx.udc.session.merge(obj)

            return f"User {ctx.udc.session.query(T).get(obj.id).first_name} successfully created and logged in"

        @rpc(M(UnsignedInteger32))
        def delUser(ctx, obj_id):
            count = ctx.udc.session.query(T).filter_by(id=obj_id).count()
            if count == 0:
                raise ResourceNotFoundError(obj_id)

            ctx.udc.session.query(T).filter_by(id=obj_id).delete()

        @rpc(_returns=Iterable(T))
        def getAllUser(ctx):
            return ctx.udc.session.query(T)

    return CrudService
