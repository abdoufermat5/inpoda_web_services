import logging

from spyne import Application
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.protocol.soap import Soap11
from spyne.server.twisted import TwistedWebResource
from spyne.server.wsgi import WsgiApplication
from twisted.internet import reactor
from twisted.web.server import Site

from db.dbInstance import TableModel
from model.User import User
from model.Tweet import Tweet
from service.userService import UserCrudService
from service.tweetService import TweetCrudService
from service.preprocessing.serviceAuthorIdentification import ServiceAuthorIdentification
from service.preprocessing.serviceExtractionHashtag import ServiceExtractionHashtag
from service.preprocessing.serviceSentimentAnalysis import ServiceSentimentAnalysis
from service.preprocessing.serviceTopicIdentification import ServiceTopicIdentification
from utils.utils import _on_method_call, _on_method_return_object, _on_method_context_closed, UserDefinedContext

user_service = UserCrudService(User, 'user')
tweet_service = TweetCrudService(Tweet, 'tweet')
application1 = Application(
    [
        user_service,
        tweet_service,
        # preprocessing services
        ServiceAuthorIdentification,
        ServiceExtractionHashtag,
        ServiceSentimentAnalysis,
        ServiceTopicIdentification,
    ],
    tns='preprocessing.inpoda.services',
    in_protocol=Soap11(validator='lxml'),
    out_protocol=Soap11(),
)

application1.event_manager.add_listener('method_call', _on_method_call)

application1.event_manager.add_listener('method_return_object',
                                        _on_method_return_object)
application1.event_manager.add_listener("method_context_closed",
                                        _on_method_context_closed)

if __name__ == '__main__':
    from wsgiref.simple_server import make_server

    wsgi_app1 = WsgiApplication(application1)
    server1 = make_server('0.0.0.0', 8000, wsgi_app1)

    TableModel.Attributes.sqla_metadata.create_all()
    logging.info("listening app1 to http://0.0.0.0:8000")
    logging.info("wsdl app1 is at: http://0.0.0.0:8000/?wsdl")

    server1.serve_forever()