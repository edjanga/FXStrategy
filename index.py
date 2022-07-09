from dash import Input, Output
from pages import HOME_APP,EDA_APP
from app import dash_app
from app import server


@dash_app.callback(
    Output(component_id='page-content',component_property='children'),
    Input(component_id='url',component_property='pathname')
)
def display_page(pathname):
    # Add proper request check, i.e. status code
    #if pathname == '/':
    #    return HOME_APP.layout
    if pathname == '/': #pages/EDA_APP.py
        return EDA_APP.dash_app.layout
    # elif pathname == '/pages/bottom_performers.py':
    #     return bottom_performers.layout
    # elif pathname == '/pages/top_performers.py':
    #     return top_performers.layout
    # elif pathname == '/pages/correlation.py':
    #     return correlation.layout
    # elif pathname == '/pages/prices.py':
    #     return prices.layout
    else:
        pass

if __name__ == '__main__':
    dash_app.run_server(debug=True,port=8050)
    # data_dummy_obj = Data()
    # p1 = multiprocessing.Process(target=data_dummy_obj.live_query, args=[universe_ls])
    # p2 = multiprocessing.Process(target=dash_app.run_server(debug=True,port=8050))
    # p1.start()
    # p2.start()
    # p1.join()
    # p2.join()