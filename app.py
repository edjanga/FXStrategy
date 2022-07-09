from flask import  Flask
from dash import Dash

def create_dash_application(flask_app):
    """
    :param flask_app:
    :return: Dash app
    """

    dash_app = Dash(server=flask_app, name='Dashboard',suppress_callback_exceptions=True,\
                    meta_tags=[{'name':'viewport', 'content':'width=device-width', 'initial-scale':1.0}])

    return dash_app

flask_app = Flask(__name__)
dash_app = create_dash_application(flask_app=flask_app)
server = dash_app.server