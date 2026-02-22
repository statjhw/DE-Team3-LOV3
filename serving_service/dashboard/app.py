import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from sqlalchemy import create_engine
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode # ★ 추가된 라이브러리

# -------------------------------------------------------------------------
# 1. 페이지 기본 설정
# -------------------------------------------------------------------------
st.set_page_config(page_title="포트홀 안전 대시보드", page_icon="🛣️", layout="wide")

# -------------------------------------------------------------------------
# 2. DB 연결 및 데이터 로딩
# -------------------------------------------------------------------------
@st.cache_resource
def init_connection():
    db_info = st.secrets["postgres"]
    engine = create_engine(
        f"postgresql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['dbname']}"
    )
    return engine

engine = init_connection()

@st.cache_data(ttl=3600)
def load_data(query):
    return pd.read_sql(query, engine)

with st.spinner('데이터를 불러오는 중입니다...'):
    df_heatmap = load_data("SELECT * FROM mvw_dashboard_heatmap;")
    df_priority = load_data("SELECT * FROM mvw_dashboard_repair_priority ORDER BY priority_rank ASC;")
    df_weekly = load_data("SELECT * FROM mvw_dashboard_weekly_stats ORDER BY dow_num ASC;")

if not df_priority.empty:
    df_priority['도로명(경도, 위도)'] = df_priority.apply(
        lambda x: f"{x['road_name']} ({x['centroid_lon']:.4f}, {x['centroid_lat']:.4f})", axis=1
    )

# -------------------------------------------------------------------------
# 3. 상태 관리 및 콜백 함수
# -------------------------------------------------------------------------
if 'dummy_requests' not in st.session_state:
    initial_dummy_data = {}
    if len(df_priority) > 5:
        initial_dummy_data[df_priority.iloc[0]['s_id']] = 2
        initial_dummy_data[df_priority.iloc[1]['s_id']] = 1
    st.session_state.dummy_requests = initial_dummy_data

def handle_repair_request(sid, road_name):
    st.session_state.dummy_requests[sid] = 2
    st.toast(f"[{road_name}] 보수 요청이 성공적으로 전달되었습니다!", icon="✅")

# -------------------------------------------------------------------------
# 4. 대시보드 UI 구성 시작
# -------------------------------------------------------------------------
st.header("🛣️ 포트홀 안전 통합 대시보드")

if not df_priority.empty and 'date' in df_priority.columns:
    latest_date = str(df_priority.iloc[0]['date'])[:10]
    st.subheader("📅 데이터 기준일 : " + latest_date + " (전일)")
else:
    st.subheader("📅 데이터 기준일 : 실시간")

st.markdown("<br>자동차의 센서 데이터와 시민 민원을 융합한 포트홀 탐지 및 보수 우선순위 분석 대시보드입니다.", unsafe_allow_html=True)

# =========================================================================
# [상단 영역] 보수 우선순위 리스트 (AgGrid로 변경됨)
# =========================================================================
st.subheader("🚨 보수 우선순위 랭킹")
st.markdown("💡 표의 **텍스트 영역 아무 곳이나 클릭**하여 행을 선택하고 보수 요청을 진행하세요. 👆")

display_df = df_priority[['priority_rank', '도로명(경도, 위도)', 'district', 'priority_score', 'complaint_count', 's_id']].copy()
display_df['request'] = display_df['s_id'].apply(lambda x: st.session_state.dummy_requests.get(x, 0))

def get_status_text(val):
    if val == 1: return "✅ 완료"
    elif val == 2: return "🚧 진행중"
    else: return "❌ 미완료"
    
display_df['진행 상태'] = display_df['request'].apply(get_status_text)

# 표에 보여줄 컬럼 지정 (s_id, request는 백그라운드에서만 사용)
show_df = display_df[['priority_rank', '도로명(경도, 위도)', 'district', 'priority_score', 'complaint_count', '진행 상태', 's_id', 'request']]
show_df.columns = ['순위', '도로명 (경도, 위도)', '관할 구역', '위험 점수', '민원(건)', '진행 상태', 's_id', 'request']

# ★ AgGrid 설정 (체크박스 완전 제거, 행 클릭 선택)
gb = GridOptionsBuilder.from_dataframe(show_df)
# 체크박스를 없애고(use_checkbox=False), 행 클릭으로만 선택되도록 설정
gb.configure_selection(selection_mode="single", use_checkbox=False) 
# 화면에 보여줄 필요 없는 데이터는 숨김 처리
gb.configure_column("s_id", hide=True)
gb.configure_column("request", hide=True)
gridOptions = gb.build()

# AgGrid 렌더링
grid_response = AgGrid(
    show_df,
    gridOptions=gridOptions,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    theme='streamlit', # 기본 테마 적용
    fit_columns_on_grid_load=True,
    height=300
)

# 선택된 행 데이터 가져오기
selected_rows = grid_response['selected_rows']
# streamlit-aggrid 버전에 따라 반환 타입이 다를 수 있어 안전하게 처리
if isinstance(selected_rows, pd.DataFrame):
    selected_data = selected_rows.to_dict('records')
else:
    selected_data = selected_rows

# [상단 제어(버튼) 영역]
btn_col1, btn_col2 = st.columns([7, 3])

with btn_col1:
    if not selected_data:
        st.info("💡 위 표에서 분석할 도로의 행을 클릭해 주세요.")
    else:
        # 선택된 행의 데이터 추출
        row_data = selected_data[0]
        selected_road_display = row_data['도로명 (경도, 위도)']
        current_status = row_data['request']
        
        if current_status == 0:
            st.info(f"현재 **{selected_road_display}** 구간은 보수 접수가 되지 않았습니다.")
        elif current_status == 2:
            st.warning(f"🚧 **{selected_road_display}** 구간은 현재 보수 공사가 진행 중입니다.")
        elif current_status == 1:
            st.success(f"✅ **{selected_road_display}** 구간은 보수 공사가 완료되었습니다.")

with btn_col2:
    if not selected_data:
        st.button("🚨 보수 요청하기", disabled=True, use_container_width=True, key="btn_default")
    else:
        row_data = selected_data[0]
        selected_sid = row_data['s_id']
        # 원래 도로명만 추출 (괄호 앞부분)
        selected_road_raw = row_data['도로명 (경도, 위도)'].split(' (')[0]
        current_status = row_data['request']
        
        if current_status == 0:
            st.button(
                f"🚨 {selected_road_raw} 보수 요청", 
                type="primary", 
                use_container_width=True, 
                on_click=handle_repair_request,
                args=(selected_sid, selected_road_raw),
                key=f"btn_req_{selected_sid}"
            )
        elif current_status == 2:
            st.button(f"요청 완료 (진행중)", disabled=True, use_container_width=True, key=f"btn_dis_2_{selected_sid}")
        elif current_status == 1:
            st.button(f"요청 완료 (완료됨)", disabled=True, use_container_width=True, key=f"btn_dis_1_{selected_sid}")

st.markdown("<br>", unsafe_allow_html=True)
st.divider()

# =========================================================================
# [하단 영역] 지도 및 통계 
# =========================================================================
col_map, col_stats = st.columns([6, 4])

with col_map:
    st.subheader("📍 도로 보수 시급도 맵") 
    
    center_lat, center_lon = 37.5665, 126.9780
    zoom_level = 11
    target_road = None
    
    # 지도 중심 이동 로직 (AgGrid 선택 데이터 활용)
    if selected_data:
        # s_id를 기준으로 원본 df_priority에서 정확한 좌표 탐색
        target_sid = selected_data[0]['s_id']
        matched_row = df_priority[df_priority['s_id'] == target_sid].iloc[0]
        
        center_lat = matched_row['centroid_lat']
        center_lon = matched_row['centroid_lon']
        zoom_level = 16 
        target_road = matched_row['road_name']

    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom_level, tiles="CartoDB positron")
    
    for _, row in df_priority.iterrows():
        score = row['priority_score']
        if score >= 1000:       
            color, radius = 'red', 12
        elif score >= 300:      
            color, radius = 'orange', 8
        else:                   
            color, radius = 'green', 5
            
        status_val = st.session_state.dummy_requests.get(row['s_id'], 0)
        status_text = "❌ 미완료" if status_val == 0 else ("🚧 진행중" if status_val == 2 else "✅ 완료")

        tooltip_html = f"""
        <b>{row['road_name']} ({row['district']})</b><br>
        - 우선순위 순위: <b>{row['priority_rank']}위</b><br>
        - 보수 시급 점수: {score}점<br>
        - 접수 민원: {row['complaint_count']}건<br>
        - <b>진행 상태: {status_text}</b>
        """
        
        folium.CircleMarker(
            location=[row['centroid_lat'], row['centroid_lon']],
            radius=radius, color=color, fill=True, fill_opacity=0.6, tooltip=tooltip_html
        ).add_to(m)

    if selected_data and target_road:
        folium.Marker(
            location=[center_lat, center_lon],
            popup=f"선택됨: {target_road}",
            icon=folium.Icon(color='red', icon='info-sign')
        ).add_to(m)

    st_folium(m, width='stretch', height=600, returned_objects=[], key="pothole_map")

with col_stats:
    st.subheader("📈 핵심 지표 요약")
    if not df_priority.empty:
        st.metric(
            label="현재 가장 위험한 도로", 
            value=df_priority.iloc[0]['road_name'], 
            delta=f"1순위 ({df_priority.iloc[0]['district']})", 
            delta_color="inverse"
        )
        
        kpi1, kpi2 = st.columns(2)
        with kpi1:
            total_complaints = int(df_priority['complaint_count'].sum())
            st.metric(label="누적 시민 민원", value=f"{total_complaints}건")
        with kpi2:
            total_impacts = int(df_priority['total_impacts'].sum())
            st.metric(label="누적 충격 감지", value=f"{total_impacts:,}회")
            
    st.divider()

    st.subheader("📊 최근 7일 요일별 통계")
    if not df_weekly.empty:
        fig = px.bar(
            df_weekly, 
            x='day_of_week', 
            y=['impact_count', 'total_count'],
            barmode='group',
            labels={'value': '건수 / 통행량', 'day_of_week': '요일', 'variable': '지표'},
            color_discrete_map={'impact_count': '#EF553B', 'total_count': '#636EFA'}
        )
        newnames = {'impact_count':'포트홀 충격 횟수', 'total_count': '차량 통행량'}
        fig.for_each_trace(lambda t: t.update(name = newnames[t.name],
                                            legendgroup = newnames[t.name],
                                            hovertemplate = t.hovertemplate.replace(t.name, newnames[t.name])))
        
        fig.update_layout(margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("최근 7일간의 통계 데이터가 없습니다.")