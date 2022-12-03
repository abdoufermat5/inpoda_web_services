import logging

from spyne import Application
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication

from db.dbInstance import TableModel
from service.analysis.serviceHashTagPublish import ServiceHashTagPublish
from service.analysis.serviceTopKHashtag import ServiceTopKHashTag
from service.analysis.serviceTopKTopics import ServiceTopKTopic
from service.analysis.serviceTopKUser import ServiceTopKUser
from service.analysis.serviceTopicPublish import ServiceTopicPublish
from service.analysis.serviceUserPublish import ServiceUserPublish
from service.userService import UserCrudService
from model.User import User
from utils.utils import _on_method_call, _on_method_return_object, _on_method_context_closed

user_service = UserCrudService(User, 'user')

application2 = Application(
    [
        user_service,
        # Analysis services
        ServiceTopKUser,
        ServiceTopKTopic,
        ServiceTopKHashTag,
        ServiceHashTagPublish,
        ServiceUserPublish,
        ServiceTopicPublish,
    ],
    tns='analysis.inpoda.services',
    in_protocol=Soap11(validator='lxml'),
    out_protocol=Soap11(),
)

application2.event_manager.add_listener('method_call', _on_method_call)

application2.event_manager.add_listener('method_return_object',
                                        _on_method_return_object)
application2.event_manager.add_listener("method_context_closed",
                                        _on_method_context_closed)

if __name__ == '__main__':
    from wsgiref.simple_server import make_server

    wsgi_app2 = WsgiApplication(application2)
    server2 = make_server('127.0.0.1', 5001, wsgi_app2)

    TableModel.Attributes.sqla_metadata.create_all()
    logging.info("listening app2 to http://127.0.0.1:5001")
    logging.info("wsdl app2 is at: http://localhost:5001/?wsdl")

    server2.serve_forever()
