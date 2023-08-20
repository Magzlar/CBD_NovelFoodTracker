import requests
import csv
import pandas as pd
import re
import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, html, dcc, Output, Input, callback
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

def update_database():
    url = r"https://data.food.gov.uk/cbd-products/id/listing.csv"
    df = retrive_clean_database(url)
    return df 

def retrive_clean_database(url: str) -> pd.DataFrame:
    '''Returns pandas dataframe from url to csv file'''
    df = pd.read_csv(url, delimiter=",", lineterminator="\n")
    df["lastUpdated"] = pd.to_datetime(df["lastUpdated"])
    df.loc[df["manufacturerSupplier"].str.contains("BRITISH CANNABIS"),
           "manufacturerSupplier",] = "CBD Health Ltd"
    return df

def make_bar_chart(x:pd.DataFrame, y:pd.DataFrame, title: str, labels: dict,xaxis_title:str = None, yaxis_title:str=None) -> px.bar:
    """Returns plotly bar chart already formatted"""
    bar_chart = px.bar(x=x, y=y, title=title, labels=labels)
    bar_chart.update_layout(xaxis_title = xaxis_title, 
                     yaxis_title = yaxis_title,
                     paper_bgcolor ="whitesmoke",
                     plot_bgcolor="whitesmoke",
                     font_family="Poppins-Light"
                     )
    return bar_chart

def make_line_graph(x:pd.DataFrame, y:pd.DataFrame, title: str, labels: dict) -> px.line:
    """Returns plotly line graph already formatted"""
    line_graph = px.line(x=x, y=y, title=title, labels=labels)
    line_graph.update_layout(xaxis_title = "",
                     yaxis_title = "Number of<br> validated and removed applications",
                     paper_bgcolor="WhiteSmoke",
                     plot_bgcolor="WhiteSmoke",
                     font_family="Poppins-Light"
                     )
    return line_graph

def categorise_product(product_name:str)->pd.DataFrame.columns:
    """Returns new df with new column product name. Uses regex to search column and label products based on matching of words"""
    if re.search("drink|water|tea|coffee|wine|vodka|lager|beer|\brum\b|\bgin\b",product_name,flags=re.IGNORECASE):
        return "Drink"
    elif re.search("chewing",product_name,flags=re.IGNORECASE):
        return "Chewing Gum"
    elif re.search("capsule|gels|pill|tabs|\bcaps\b|tablets|caplets",product_name,flags=re.IGNORECASE):
        return "Capsule"
    elif re.search("oil|ml|spray|drops|tincture|mct",product_name,flags=re.IGNORECASE):
        return "Oil"
    elif re.search("edible|gummy|gummies|chocolate|caramel|sweet|candy|nuts|jam|bars|cream|sherbert|popcorn|jelly|brownies|peanut|\bar\b|bites",
                   product_name,flags=re.IGNORECASE):
        return "Edible"
    elif re.search("isolate|Distillate|crystal", product_name, flags=re.IGNORECASE):
        return "isolate"
    else:
        return "Other"

def refine_category(row):  # check 'productSizeVolumeQuantity' to overwrite values that couldn't be categorised in itntial screening function to overwrite the current value if productCategory is == to "other"
    if row["ProductCategory"] == "Other":
        return categorise_product(str(row["productSizeVolumeQuantity"]))
    else:
        return row["ProductCategory"]

url = r"https://data.food.gov.uk/cbd-products/id/listing.csv"
df = retrive_clean_database(url)
scheduler = BackgroundScheduler()
scheduler.add_job(update_database, 'interval', minutes=15)  # Run the update every 15 minutes
scheduler.start()

#### Graph 2 ####
num_days = (df["lastUpdated"].max() - df["lastUpdated"].min()).days # Calculate how long applications have been processed for
remaining_applications = len(df) - len(df[(df["status"] == "Validated") | (df["status"] == "Removed")]) # All applications minus validated and removed applications
average_processing_rate = remaining_applications / num_days
days_required = remaining_applications/average_processing_rate
predicted_finish_date = df['lastUpdated'].max() + pd.DateOffset(days=days_required)
filtered_df2 = df[(df["status"] == "Validated") | (df["status"] == "Removed")] 
grouped_df2 = filtered_df2.groupby('lastUpdated').size() 

line_chart1 = make_line_graph(x=grouped_df2.index,
                                y=grouped_df2.values,
                                title=f"At the current rate of validating or removing applications all applications will be processed by <b>{predicted_finish_date.date().strftime('%d/%m/%y')}</b>",
                                labels={"x": "date ", "y": "Processed applications "},
                                )

#### Graph 3 ####
bar_chart1 = make_bar_chart(x=df["manufacturerSupplier"].value_counts().index[:10],
                            y=df["manufacturerSupplier"].value_counts().values[:10],
                            title=f"<b>10</b> companies are responsible for <b>{sum(df['manufacturerSupplier'].value_counts().head(10))/len(df)*100:.1f}%</b> of all applications",
                            labels={"x": "Company name ", "y": "Number of applciations "},
                            )

#### Graph 4 ####
grouped_df = df.groupby("manufacturerSupplier")["status"].value_counts().unstack()
sorted_group_values = grouped_df.sort_values(by="Validated",ascending=False) # Sort by companies with most validated applications
bar_chart2 = make_bar_chart(x=sorted_group_values.index[:10],
                            y=sorted_group_values["Validated"][:10],
                            title=f'Of <b>{df["status"].value_counts()[1]}</b> validated applications, <b>10</b> companies hold <b>{sum(list(grouped_df.sort_values(by="Validated",ascending=False)["Validated"][:10]))/df["status"].value_counts()[1]*100:.1f}%</b>',
                            labels={"x": "Company name ", "y": "Number of applications "},
                            )
#### Graph 5 ####
df["ProductCategory"] = df["productName"].apply(categorise_product)
df["ProductCategory"] = df.apply(refine_category, axis=1)
product_categories = df["ProductCategory"].value_counts()

bar_chart3 = make_bar_chart(x=product_categories.index,
                            y=product_categories.values,
                            title="<b>Oils</b> repersent the bulk CBD novel food applications",
                            labels={"x": "Product type ", "y": "Number of products "},
                            )

#### Graph 6 ####
product_names = df['productName']
pattern = r'(\d+)\s*mg'
extracted_values = product_names.str.extract(pattern, expand=False) # Extracting "mg" from each product name
df["ExtractedValue"] = extracted_values

bar_chart4 = make_bar_chart(x = list(map(str,df["ExtractedValue"].value_counts(sort=True)[:10].index)),
                            y=df["ExtractedValue"].value_counts(sort=True)[:10].values,
                            title=f'<b>{df["ExtractedValue"].value_counts().index[0]}mg</b> is the most common amount found in CBD products',
                            labels={"x": "Amount (mg) ", "y": "Number of products "}
                            )

app = Dash(__name__,meta_tags=[{"name":"viewport","content":"width=device-width,initial-scale=1.0,maximum-scale=1.2,minimum-scale=0.5"}],title="CBD Tracker")
server = app.server
app.layout = html.Div(
    children=[html.H1(
        children=["CBD Novel Food Applications Tracker ",
                                html.Br(),f"(Live Updates)"],
                                id="title",
                                className="header-title"
                                ),

            html.P(
                children=["To ensure regulatory compliance in the UK's CBD market, the",
                                html.A(" Food Standards Agency (FSA)",
                                    href="https://www.food.gov.uk/business-guidance/cbd-products-linked-to-novel-food-applications",
                                    target="_blank"
                                    )," required that all companies involved in the sale of CBD products, which were already on the market before 13/02/20, submit novel food applications by 31/03/21. At this stage, applications granted the status 'validated' proceed to the risk assessment phase, while applications marked as 'removed' are not allowed for sale in the UK."],
                                    className="header-description"
                                    ),

            html.P(
                children=["What does the CBD novel food applications tracker do?"],
                className="header-description2"
            ),
            
            html.P(children = [html.Ol(children=[html.B("Real Time: "),"CBD NFA tracker checks for updates to the FSA database every 15 minutes"]),
                    html.Ol(children =[html.B("Comprehensive Overview:"), " Get a clear overview of the current status of all applications."]),
                    html.Ol(children = [html.B("Estimated Finish Date:"), " Get an estimate of when all applications are likely to be processed based on historical data."]),
                    html.Ol(children = [html.B("Company Search:")," Easily find the application status for any company that has applied for a CBD novel foods license."]),
                    html.Ol(children = [html.B("Key Players:"), " Identify the companies with the highest number of applications."]),
                    html.Ol(children = [html.B("Product Variety:"), " Insights into the different types of CBD products submitted for novel food applications."])],
                    className="header-description3"
            ),

        html.P(
            children=[f"",
                html.A("FSA database of CBD novel food applications",
                        href="https://data.food.gov.uk/cbd-products/products-list",
                        style={"color": "greenyellow"}
                        ),
                        " last updated: ",
                html.B(f"{df['lastUpdated'].max().date().strftime('%d/%m/%y')}",
                        style={"color": "greenyellow"}
                        ),
            ],
            className="header-description4",
        ),
        html.H2(children=["Search a company to view the status of all their applications"],
                className= "header-description5",
        ),
        dcc.Dropdown(id="manufacturer-dropdown",
                        options=[{"label": supplier, "value": supplier} for supplier in df["manufacturerSupplier"].unique()],
                        placeholder=f"{len(df['manufacturerSupplier'].unique())} companies, {len(df)} applications",
                        style={"width": "65.80%", "background-color": "whitesmoke"},
                        value=None),

    
        dcc.Graph(id='status-bar-chart', style={"border-bottom": "10px solid lightsteelblue","max-width":"1350px"}),
        dcc.Graph(id="Graph2", figure=line_chart1,style={"border-bottom": "10px solid lightsteelblue","max-width":"1350px"}),
        dcc.Graph(id="graph4", figure= bar_chart1, style={"border-bottom": "10px solid lightsteelblue","max-width":"1350px"}),
        dcc.Graph(id="graph5", figure=bar_chart2, style={"border-bottom": "10px solid lightsteelblue","max-width":"1350px"}),
        dcc.Graph(id="graph6", figure=bar_chart3, style={"border-bottom": "10px solid lightsteelblue","max-width":"1350px"}),  # Graph 5 | Types of CBD products
        dcc.Graph(id="graph7", figure=bar_chart4, style={"border-bottom": "10px solid lightsteelblue","max-width":"1350px"}),
    ],
    className="header")


@app.callback(Output('status-bar-chart', 'figure'),
              [Input('manufacturer-dropdown', 'value')])

def update_bar_chart(manufacturer):
    if manufacturer is not None:
        filtered_df = df[df['manufacturerSupplier'] == manufacturer]
        counts = filtered_df['status'].value_counts()
        fig = px.pie(data_frame=counts,
                 names = counts.index, 
                 values = counts.values,
                 color = counts.index,
                 color_discrete_map={"Validated":"green","Awaiting evidence":"blue","Removed":"#D62728"}
                 )
    
        fig.update_layout(plot_bgcolor="whitesmoke",  # Set the plot background color
                        paper_bgcolor="whitesmoke",
                        legend=dict(y=0.95,x=0.12,font=dict(size=22)),
                        font_family="Poppins-Light",
                        )
        fig.update_traces(hovertemplate='Status: %{label}<br>Applications: %{value}')
        return fig
    else:
        counts = df['status'].value_counts()
        fig = px.pie(data_frame=counts,names = counts.index, 
                 values = counts.values,
                 color = counts.index,
                 color_discrete_map={"Validated":"green","Awaiting evidence":"blue","Removed":"#D62728"}
                 )
        
        fig.update_layout(plot_bgcolor="whitesmoke",  # Set the plot background color
                          paper_bgcolor="whitesmoke",
                          legend=dict(y=0.95,x=0.12,font=dict(size=22)),
                          font_family="Poppins-Light",
                          )
        
        fig.update_traces(hovertemplate='Status: %{label}<br>Applications: %{value}')
    
        return fig
        
html.Div(
    [
        # ... Your existing layout elements ...

        # Google Analytics tracking code
        html.Script(
            '''
            <!-- Google tag (gtag.js) -->
            <script async src="https://www.googletagmanager.com/gtag/js?id=UA-131426683-1"></script>
            <script>
              window.dataLayer = window.dataLayer || [];
              function gtag(){dataLayer.push(arguments);}
              gtag('js', new Date());

              gtag('config', 'UA-131426683-1');
            </script>
            '''
        )
    ],
    className="header"
)

if __name__ == "__main__":
    app.run(debug=True)
