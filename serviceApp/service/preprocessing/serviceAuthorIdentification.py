from spyne import ServiceBase, srpc, String, UnsignedInteger, UnsignedInteger32, rpc, Unicode

from model.Tweet import Tweet


class ServiceAuthorIdentification(ServiceBase):

    @rpc(Unicode, _returns=String)
    def identifyAuthor(ctx,tweet_id):
        # get tweet from database
        tweet = ctx.udc.session.query(Tweet).filter_by(id=tweet_id).one()
        return f"AUTHOR of tweet **{tweet.text}** is {tweet.author_id}"