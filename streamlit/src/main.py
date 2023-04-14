import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from psql_connect import extract_data
import numpy as np


# #extract data from PostgreSQL
ls_df = extract_data()
l_season = ls_df[0]
p_season = ls_df[1]
p_match = ls_df[2]
st.set_page_config(page_title = 'Dashboard Football', 
    layout='wide',
    page_icon='chart_with_upwards_trend')


#Overview
def overview(table: pd.DataFrame, detail: str):
    if (st.checkbox('Do you want to see Data ?')):
        table
    col1, col2 = st.columns(2)
    co_df = table.columns.to_list()
    with col1:
        st.bar_chart(table.describe())
        if (st.checkbox('Do you want to see describe each column ?')):
            for col in co_df:
                    if table[col].dtypes not in ['int64', 'float64']:
                        continue
                    st.bar_chart(table[col].describe())
    with col2:
        st.caption(f':red[Columns]: {len(co_df)}')
        st.caption(f':red[Records]: {len(table)}')
        st.caption(f':red[Description]: {detail}')
        st.caption(f':red[Columns name]:{co_df}')

#league statistic
def statleague():
    Cards = l_season[['name','season','yellowCards', 'redCards', 'fouls']]
    #Card_fouls
    col1, col2 = st.columns(2)
    with col1:
        #Goals per games
        fig = px.bar(l_season, x="name", y="goalPerGame", color="name", barmode="stack",
                     facet_col="season", 
                     labels={"name": "League", "goals/games": "GPG"})
        fig.update_layout(showlegend=False,
                        title='Goals per Game')
        st.plotly_chart(fig)
        #fouls
        fig = px.line(Cards, x='season', y='fouls', color='name')
        fig.update_layout(title='Fouls of leagues',
                        xaxis_title='Season',
                        yaxis_title='Fouls',
                        legend_title='League')
        st.plotly_chart(fig)
    with col2:
        #Red card
        fig = px.line(Cards, x='season', y='redCards', color='name')
        fig.update_layout(title='Red Cards of leagues',
                        xaxis_title='Season',
                        yaxis_title='RedCards',
                        legend_title='League')
        st.plotly_chart(fig)
        #yellow card
        fig = px.line(Cards, x='season', y='yellowCards', color='name')
        fig.update_layout(title='Yellow Cards of leagues',
                        xaxis_title='Season',
                        yaxis_title='YellowCards',
                        legend_title='League')
        st.plotly_chart(fig)


#Player statistic
def statplayer():
    col1, col2 = st.columns(2)
    with col1:
        #Best offensive player
        top_player90= p_match[(p_match['goalsPer90'] > 0.8) | (p_match['assistsPer90'] > 0.4)]

        fig = px.scatter(p_match[['name','goalsPer90', 'assistsPer90']], x='goalsPer90', y='assistsPer90', hover_name='name')
        fig.add_trace(
            go.Scatter(x=top_player90['goalsPer90'], y=top_player90['assistsPer90'],
                       mode='markers+text', marker_size=5, text=top_player90['name'],
                       textposition='bottom center', textfont=dict(size=15))
        )
        fig.update_layout(title='Best offensive Players (2018-2020)', xaxis_title='Goals Per 90min', yaxis_title='Assists Per 90min')
        st.plotly_chart(fig)

        #goals-xgoal
        fig = px.scatter(p_season, x="xGoals", y="goals", color=(p_season['xGoals'] - p_season['goals'] < 10),
                         color_discrete_sequence=["red", "green"], opacity=0.5)
        fig.update_layout(title="Goals (G) and Expected Goals (xG)",
                          xaxis_title="xG",
                          yaxis_title="G",
        ) 
        st.plotly_chart(fig)
        

    with col2:
        #Top score player
        topPlayer = p_season.groupby(['name']).agg({'goals': 'sum'}).sort_values('goals', ascending=False).reset_index()
        topPlayer = p_season[p_season['name'].isin(topPlayer.name[:5])]

        fig = px.line(topPlayer, x='season',y='goals',color='name')
        fig.update_layout(
                        title='Top score player (season 2014 - 2020)',
                        xaxis_title='Season',
                        yaxis_title='Goals',
                        legend_title='PLayers'
        )
        st.plotly_chart(fig)

# Page status
st.sidebar.markdown("# Main page")
introduction = '''
Đây là trang thông tin tổng quan về phân tích dữ \
liệu của **dataset Football**.

Dữ liệu được load từ database __PostgreSQL__ (localhost:5432) để làm phân tích.

'''
st.sidebar.write(introduction)

st.markdown('# Football Analysis')
'''
Tổng hợp phân tích data được transform từ 5 giải vô dịch quốc gia hàng đầu châu Âu từ mùa 2014 - 2020 \
từ bộ dataset **Football**
'''
with st.container():

    st.markdown("<hr/>", unsafe_allow_html=True)
    ## Data overview
    st.markdown("## Data Overview")

    first_col, second_col = st.columns(2)

    with first_col:
        st.markdown("**Tables**")
        num=len(ls_df)
        st.markdown(f"<h2 style='text-align: left; color: red;'>{num}</h2>", unsafe_allow_html=True)

    with second_col:
        st.markdown("**DataBase**")
        st.markdown(f"**PostgreSQL**")

    if st.checkbox('Click to overview detail tables'):
        option = st.selectbox(
            'Choose table you want to see ?',
             ['statsPerLeagueSeason', 'statsPerPlayerSeason', 'statsPlayerPer90'])
        if option == 'statsPerLeagueSeason':
            detail = '''
            Đây là bảng mô tả thống kê của các giải đấu từ mùa 2014 - 2020
            '''
            overview(l_season, detail)
        elif option == 'statsPerPlayerSeason':
            detail='''
            Bảng dữ liệu thống kê các chỉ số của một cầu thủ đạt được trong từng mùa giải ở cả 5 giải đấu
            '''
            overview(p_season, detail)
        else:
            detail='''
            Bảng dữ liệu thống kê chỉ số của một cầu thủ trong 90 phút ở tất cả các mùa giải ở tất cả giải đấu

            '''
            overview(p_match,detail)



### Statistic 
with st.container():
    st.markdown("<hr/>", unsafe_allow_html=True)

    st.markdown("## Football Statistic")
    option = st.selectbox(
        '**Choose statistic**',
        ['Leagues', 'Players']
    )
    if option == 'Leagues':
        statleague()
    else:
        statplayer()
