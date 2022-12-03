from spyne import ServiceBase, Unicode, Iterable, rpc

from model.Tweet import Tweet


class ServiceUserPublish(ServiceBase):
    @rpc(Unicode, _returns=Iterable(Unicode))
    def publishByUser(ctx, user_id):
        tweets = ctx.udc.session.query(Tweet).filter_by(author_id=user_id).all()
        yield f"Found {len(tweets)} tweets by user {user_id}"
        for t in tweets:
            yield "--> " + t.text
