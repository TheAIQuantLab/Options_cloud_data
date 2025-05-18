from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
import pandas as pd
import requests

AWS_URL = "https://ry3t10t5l2.execute-api.eu-north-1.amazonaws.com/Prod"

# --- Replace with your API base URL ---
API_BASE_URL = AWS_URL

app = Dash(__name__)

app.layout = html.Div([
    html.H1("Opciones: Volatilidad Implícita vs. Precio de Ejercicio"),
    html.Div([
        html.Label("Día de Ejecución:"),
        dcc.Dropdown(
            id='execution-date-dropdown',
            options=[],
            value=None,
            placeholder="Selecciona un día de ejecución"
        ),
    ], style={'width': '48%', 'display': 'inline-block'}),

    html.Div([
        html.Label("Fecha de Expiración:"),
        dcc.Dropdown(
            id='expiration-date-dropdown',
            options=[],
            value=None,
            placeholder="Selecciona una fecha de expiración"
        ),
    ], style={'width': '48%', 'float': 'right', 'display': 'inline-block'}),

    html.Div([
        html.Label("Tipo de Opción:"),
        dcc.RadioItems(
            id='option-type-radio',
            options=[
                {'label': 'CALL', 'value': 'Call'},
                {'label': 'PUT', 'value': 'Put'}
            ],
            value=None,
            inline=True
        ),
    ], style={'padding': '10px'}),

    dcc.Graph(id='iv-strike-graph')
])

@app.callback(
    Output('execution-date-dropdown', 'options'),
    Input('execution-date-dropdown', 'search_value')
)
def update_execution_dates(search_value):
    try:
        response = requests.get(f"{API_BASE_URL}/execution-days")
        response.raise_for_status()
        execution_days = response.json()
        if search_value:
            execution_days = [date for date in execution_days if search_value in date]
        return [{'label': date, 'value': date} for date in execution_days]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching execution days: {e}")
        return []

@app.callback(
    Output('expiration-date-dropdown', 'options'),
    Input('execution-date-dropdown', 'value'),
    Input('expiration-date-dropdown', 'search_value')
)
def update_expiration_dates(selected_execution_date, search_value):
    if selected_execution_date:
        try:
            response = requests.get(f"{API_BASE_URL}/expiration_dates?execution_date={selected_execution_date}")
            response.raise_for_status()
            expiration_dates = response.json()
            if search_value:
                expiration_dates = [date for date in expiration_dates if search_value in date]
            return [{'label': date, 'value': date} for date in expiration_dates]
        except requests.exceptions.RequestException as e:
            print(f"Error fetching expiration dates: {e}")
            return []
    return []

@app.callback(
    Output('iv-strike-graph', 'figure'),
    Input('execution-date-dropdown', 'value'),
    Input('expiration-date-dropdown', 'value'),
    Input('option-type-radio', 'value')
)
def update_iv_graph(selected_execution_date, selected_expiration_date, selected_option_type):
    if selected_execution_date:
        query_params = f"execution_date={selected_execution_date}"
        if selected_expiration_date:
            query_params += f"&expiration_date={selected_expiration_date}"
        if selected_option_type:
            query_params += f"&type_cp={selected_option_type}"

        try:
            response = requests.get(f"{API_BASE_URL}/ivs?{query_params}")
            response.raise_for_status()
            data = response.json()
            df = pd.DataFrame(data)
            df['strike_price'] = df['strike_price'].str.replace(".", "").str.replace(",", ".").astype(float)
            df['IV'] = df['IV'].astype(float)
            df['T'] = df['T'].astype(float)

            if not df.empty:
                fig = go.Figure(data=[go.Scatter(x=df['strike_price'], y=df['IV'], mode='markers+lines')])
                fig.update_layout(
                    title=f'Volatilidad Implícita vs. Precio de Ejercicio<br>({selected_execution_date}, {selected_expiration_date}, {selected_option_type})',
                    xaxis_title='Precio de Ejercicio (Strike)',
                    yaxis_title='Volatilidad Implícita (IV)'
                )
                return fig
            else:
                return {
                    'data': [],
                    'layout': {
                        'title': 'No hay datos disponibles con los filtros seleccionados',
                        'xaxis': {'title': 'Precio de Ejercicio (Strike)'},
                        'yaxis': {'title': 'Volatilidad Implícita (IV)'}
                    }
                }
        except requests.exceptions.RequestException as e:
            print(f"Error fetching IV data: {e}")
            return {
                'data': [],
                'layout': {
                    'title': 'Error al obtener los datos de la API',
                    'xaxis': {'title': 'Precio de Ejercicio (Strike)'},
                    'yaxis': {'title': 'Volatilidad Implícita (IV)'}
                }
            }
    else:
        return {
            'data': [],
            'layout': {
                'title': 'Selecciona un día de ejecución para mostrar los datos',
                'xaxis': {'title': 'Precio de Ejercicio (Strike)'},
                'yaxis': {'title': 'Volatilidad Implícita (IV)'}
            }
        }

if __name__ == '__main__':
    app.run(debug=True)
