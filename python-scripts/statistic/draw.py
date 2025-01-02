from dash import Dash, dcc, html, Input, Output, State, callback_context
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
import json

# Đọc dữ liệu từ các file CSV
df_trends = pd.read_csv("property_data_10_years.csv")
df_types = pd.read_csv("property_type.csv")
df_county = pd.read_csv("property_county.csv")

# Xử lý dữ liệu cho biểu đồ donut
old_new_data = df_types[df_types['Type'].isin(['Old', 'New'])]
tenure_data = df_types[df_types['Type'].isin(['Freehold', 'Leasehold'])]

# Tạo ứng dụng Dash
app = Dash(__name__)

# Định nghĩa CSS styles
styles = {
    'container': {
        'max-width': '1200px',
        'margin': '0 auto',
        'padding': '20px',
        'font-family': 'Arial, sans-serif'
    },
    'header': {
        'backgroundColor': '#f8f9fa',
        'padding': '20px',
        'marginBottom': '30px',
        'borderRadius': '10px',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
    },
    'title': {
        'textAlign': 'center',
        'color': '#2c3e50',
        'marginBottom': '10px',
        'fontSize': '32px'
    },
    'subtitle': {
        'textAlign': 'center',
        'color': '#7f8c8d',
        'marginBottom': '20px',
        'fontSize': '18px'
    },
    'graph-container': {
        'backgroundColor': 'white',
        'padding': '20px',
        'marginBottom': '30px',
        'borderRadius': '10px',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
    },
    'nav-button': {
        'backgroundColor': '#3498db',
        'color': 'white',
        'padding': '10px 20px',
        'border': 'none',
        'borderRadius': '5px',
        'cursor': 'pointer',
        'marginRight': '10px',
        'marginBottom': '20px',
        'fontSize': '16px'
    }
}

# Tạo layout cho trang trends
def create_trends_layout():
    return [
        # Header
        html.Div(style=styles['header'], children=[
            html.H1("Phân tích Bất động sản 10 năm gần nhất", style=styles['title']),
            html.P("Theo dõi biến động số lượng giao dịch và giá trị bất động sản qua các năm", 
                   style=styles['subtitle'])
        ]),
        
        # Biểu đồ số lượng giao dịch
        html.Div(style=styles['graph-container'], children=[
            html.H2("Số lượng Giao dịch theo Năm", 
                    style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '20px'}),
            dcc.Graph(
                figure={
                    "data": [
                        go.Bar(
                            x=df_trends["Year"],
                            y=df_trends["Number_of_Transactions"],
                            name="Số lượng Giao dịch",
                            marker_color='#3498db'
                        )
                    ],
                    "layout": go.Layout(
                        xaxis={"title": "Năm"},
                        yaxis={"title": "Số lượng Giao dịch"},
                        showlegend=False,
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                    )
                }
            )
        ]),
        
        # Biểu đồ tổng giá trị
        html.Div(style=styles['graph-container'], children=[
            html.H2("Tổng Giá trị Giao dịch theo Năm", 
                    style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '20px'}),
            dcc.Graph(
                figure={
                    "data": [
                        go.Bar(
                            x=df_trends["Year"],
                            y=df_trends["Total_Value"],
                            name="Tổng Giá trị",
                            marker_color='#2ecc71'
                        )
                    ],
                    "layout": go.Layout(
                        xaxis={"title": "Năm"},
                        yaxis={"title": "Tổng Giá trị (£)"},
                        showlegend=False,
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                    )
                }
            )
        ])
    ]

# Tạo layout cho trang types
def create_types_layout():
    return [
        # Header
        html.Div(style=styles['header'], children=[
            html.H1("Phân tích Loại hình Bất động sản", style=styles['title']),
            html.P("Phân bổ theo loại hình và hình thức sở hữu", style=styles['subtitle'])
        ]),
        
        # Container cho 2 biểu đồ donut
        html.Div(style={'display': 'flex', 'justifyContent': 'space-between'}, children=[
            # Biểu đồ Old vs New
            html.Div(style={**styles['graph-container'], 'width': '48%'}, children=[
                html.H2("Phân bổ BĐS Cũ và Mới", 
                        style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '20px'}),
                dcc.Graph(
                    figure=go.Figure(data=[go.Pie(
                        labels=old_new_data['Type'],
                        values=old_new_data['Number of Transactions'],
                        hole=.4,
                        marker_colors=['#3498db', '#2ecc71']
                    )])
                )
            ]),
            
            # Biểu đồ Freehold vs Leasehold
            html.Div(style={**styles['graph-container'], 'width': '48%'}, children=[
                html.H2("Phân bổ Hình thức Sở hữu", 
                        style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '20px'}),
                dcc.Graph(
                    figure=go.Figure(data=[go.Pie(
                        labels=tenure_data['Type'],
                        values=tenure_data['Number of Transactions'],
                        hole=.4,
                        marker_colors=['#e74c3c', '#f1c40f']
                    )])
                )
            ])
        ])
    ]

def create_map_layout():
    avg_value = df_county['average'].mean()
    
    fig = px.density_mapbox(
        df_county, 
        lat='Latitude',
        lon='Longitude',
        z='average',            # Đổi từ sum sang average
        radius=50,              # Tăng bán kính
        center=dict(lat=53.5, lon=-2),
        zoom=5.5,
        mapbox_style='open-street-map',
        height=600,
        color_continuous_scale=[
            [0, 'rgb(0,128,0)'],      # green
            [0.5, 'rgb(0,0,255)'],     # blue
            [1, 'rgb(128,0,128)']      # purple
        ],
        opacity=1,  
        hover_data={
            'County': True,
            'average': ':,.0f'  # Format số để dễ đọc
        },
        labels={'average': 'Average Value'}  # Label cho giá trị average
    )

    fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0}
    )

    return [
        html.Div(style=styles['header'], children=[
            html.H1("Phân bố Giá trị Trung bình Bất động sản theo Khu vực", style=styles['title']),
            html.P(f"Giá trị trung bình toàn quốc: £{avg_value:,.0f}", style=styles['subtitle'])
        ]),
        
        html.Div(style=styles['graph-container'], children=[
            dcc.Graph(figure=fig)
        ]),
        
        html.Div(style=styles['graph-container'], children=[
            html.H2("Chi tiết theo Khu vực", 
                    style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '20px'}),
            html.Table(
                children=[
                    html.Tr([html.Th(col) for col in ['Khu vực', 'Giá trị Trung bình']]),
                    *[
                        html.Tr([
                            html.Td(row['County']),
                            html.Td(f"£{row['average']:,.0f}")
                        ]) for index, row in df_county.sort_values(by='average', ascending=False).iterrows()
                    ]
                ],
                style={
                    'width': '100%',
                    'border-collapse': 'collapse',
                    'text-align': 'left'
                }
            )
        ])
    ]

# Layout chính của ứng dụng
app.layout = html.Div(style=styles['container'], children=[
    dcc.Store(id='current-page', data='trends'),
    html.Div(style={'display': 'flex', 'marginBottom': '20px'}, children=[
        html.Button(
            'Xem Phân tích Theo Năm',
            id='btn-trends',
            style=styles['nav-button']
        ),
        html.Button(
            'Xem Phân tích Loại hình',
            id='btn-types',
            style=styles['nav-button']
        ),
        html.Button(
            'Xem Bản đồ Khu vực',
            id='btn-map',
            style=styles['nav-button']
        ),
    ]),
    html.Div(id='page-content')
])

# Callback để xử lý điều hướng
@app.callback(
    Output('page-content', 'children'),
    [Input('btn-trends', 'n_clicks'),
     Input('btn-types', 'n_clicks'),
     Input('btn-map', 'n_clicks')]
)
def display_page(btn1, btn2, btn3):
    ctx = callback_context
    if not ctx.triggered:
        return create_trends_layout()
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        if button_id == 'btn-trends':
            return create_trends_layout()
        elif button_id == 'btn-types':
            return create_types_layout()
        elif button_id == 'btn-map':
            return create_map_layout()
    return create_trends_layout()

# Chạy ứng dụng
if __name__ == "__main__":
    app.run_server(debug=True)