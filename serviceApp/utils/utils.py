from db.dbInstance import Session


class UserDefinedContext(object):
    def __init__(self):
        self.session = Session()


def _on_method_call(ctx):
    ctx.udc = UserDefinedContext()


def _on_method_return_object(ctx):
    ctx.udc.session.commit()


def _on_method_context_closed(ctx):
    if ctx.udc is not None:
        ctx.udc.session.close()