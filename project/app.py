import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import plotly.figure_factory as ff

# Page configuration
st.set_page_config(
    page_title="Delhivery Logistics Dashboard", 
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🚚"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
    }
    .sidebar-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #333;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Connect to PostgreSQL
@st.cache_resource
def init_connection():
    return create_engine("postgresql+psycopg2://delhivery_user:temp123@localhost:5432/logistics_db")

engine = init_connection()

# Enhanced data loading with error handling
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    try:
        query = """
        SELECT f.*, 
               src.center_name AS source_name,
               src.center_code AS source_code,
               dest.center_name AS dest_name,
               dest.center_code AS dest_code,
               d.full_date,
               d.day_of_week,
               d.is_weekend,
               d.month,
               d.year,
               v.vehicle_type
        FROM fact_trips f
        JOIN dim_location src ON f.source_location_id = src.location_id
        JOIN dim_location dest ON f.destination_location_id = dest.location_id
        JOIN dim_date d ON f.date_id = d.date_id
        JOIN dim_vehicles v ON f.vehicle_id = v.vehicle_id
        """
        df = pd.read_sql(query, engine)
        df['full_date'] = pd.to_datetime(df['full_date'])
        df['route'] = df['source_name'] + ' → ' + df['dest_name']
        df['efficiency_ratio'] = df['osrm_time'] / df['actual_time'].replace(0, np.nan)
        df['distance_efficiency'] = df['osrm_distance'] / df['actual_distance_to_destination'].replace(0, np.nan)
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# Load additional summary data
@st.cache_data(ttl=300)
def load_summary_stats():
    try:
        queries = {
            'daily_stats': """
                SELECT d.full_date, 
                       COUNT(*) as trip_count,
                       AVG(f.time_deviation) as avg_deviation,
                       SUM(CASE WHEN f.is_cutoff THEN 1 ELSE 0 END) as cutoff_count,
                       AVG(f.actual_distance_to_destination) as avg_distance
                FROM fact_trips f
                JOIN dim_date d ON f.date_id = d.date_id
                GROUP BY d.full_date
                ORDER BY d.full_date
            """,
            'route_performance': """
                SELECT src.center_name as source, dest.center_name as destination,
                       COUNT(*) as trip_count,
                       AVG(f.time_deviation) as avg_deviation,
                       AVG(f.actual_time) as avg_actual_time,
                       AVG(f.osrm_time) as avg_predicted_time,
                       SUM(CASE WHEN f.is_cutoff THEN 1 ELSE 0 END) as cutoff_violations
                FROM fact_trips f
                JOIN dim_location src ON f.source_location_id = src.location_id
                JOIN dim_location dest ON f.destination_location_id = dest.location_id
                GROUP BY src.center_name, dest.center_name
                HAVING COUNT(*) > 10
                ORDER BY trip_count DESC
            """
        }
        
        results = {}
        for key, query in queries.items():
            results[key] = pd.read_sql(query, engine)
            
        return results
    except Exception as e:
        st.error(f"Error loading summary stats: {e}")
        return {}

# Main app
def main():
    # Header
    st.markdown('<h1 class="main-header">🚚 Delhivery Logistics Intelligence Dashboard</h1>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading data..."):
        df = load_data()
        summary_stats = load_summary_stats()
    
    if df.empty:
        st.error("No data available. Please check your database connection.")
        return
    
    # Sidebar filters
    st.sidebar.markdown(
    '<div class="sidebar-header" style="color: white;">📊 Filters & Controls</div>',
    unsafe_allow_html=True
)

    # Date range filter
    min_date = df['full_date'].min().date()
    max_date = df['full_date'].max().date()
    
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Route type filter
    route_types = ['All'] + sorted(df['route_type'].unique().tolist())
    selected_route_type = st.sidebar.selectbox("Route Type", route_types)
    
    # Source/Destination filters
    sources = ['All'] + sorted(df['source_name'].unique().tolist())
    destinations = ['All'] + sorted(df['dest_name'].unique().tolist())
    
    selected_source = st.sidebar.selectbox("Source Center", sources)
    selected_destination = st.sidebar.selectbox("Destination Center", destinations)
    
    # Performance threshold
    deviation_threshold = st.sidebar.slider("Time Deviation Threshold (minutes)", 
                                          min_value=0, max_value=int(df['time_deviation'].max()), 
                                          value=30)
    
    # Apply filters
    filtered_df = df.copy()
    
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df['full_date'].dt.date >= start_date) & 
            (filtered_df['full_date'].dt.date <= end_date)
        ]
    
    if selected_route_type != 'All':
        filtered_df = filtered_df[filtered_df['route_type'] == selected_route_type]
    
    if selected_source != 'All':
        filtered_df = filtered_df[filtered_df['source_name'] == selected_source]
        
    if selected_destination != 'All':
        filtered_df = filtered_df[filtered_df['dest_name'] == selected_destination]
    
    # Key Performance Indicators
    st.markdown("## 📈 Key Performance Indicators")
    
    if len(filtered_df) > 0:
        col1, col2, col3, col4, col5 = st.columns(5)
        
        # Calculate KPIs
        total_trips = len(filtered_df)
        on_time_pct = 100 * len(filtered_df[filtered_df["is_cutoff"] == 0]) / total_trips if total_trips > 0 else 0
        avg_deviation = filtered_df["time_deviation"].mean()
        cutoff_count = filtered_df["is_cutoff"].sum()
        avg_efficiency = filtered_df['efficiency_ratio'].mean() * 100 if filtered_df['efficiency_ratio'].notna().any() else 0
        
        with col1:
            st.metric("✅ On-Time Delivery", f"{on_time_pct:.1f}%", 
                     delta=f"{on_time_pct-95:.1f}%" if on_time_pct < 95 else f"+{on_time_pct-95:.1f}%")
        
        with col2:
            st.metric("📦 Total Trips", f"{total_trips:,}")
        
        with col3:
            st.metric("⚠️ Cutoff Violations", f"{cutoff_count:,}", 
                     delta=f"{cutoff_count}" if cutoff_count > 0 else "0")
        
        with col4:
            st.metric("⏱️ Avg Time Deviation", f"{avg_deviation:.1f} min", 
                     delta=f"{avg_deviation:.1f} min" if avg_deviation > 0 else f"{avg_deviation:.1f} min")
        
        with col5:
            st.metric("🎯 Route Efficiency", f"{avg_efficiency:.1f}%",
                     delta=f"{avg_efficiency-100:.1f}%" if avg_efficiency != 0 else "N/A")
    
    st.markdown("---")
    
    # Main dashboard tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Overview", "🗺️ Route Analysis", "📈 Performance Trends", 
        "🚛 Operations", "📋 Data Quality", "🔍 Deep Dive"
    ])
    
    with tab1:
        # Overview tab
        col1, col2 = st.columns(2)
        
        with col1:
            # Time deviation distribution
            fig_hist = px.histogram(
                filtered_df, 
                x='time_deviation', 
                nbins=50,
                title="Distribution of Time Deviations",
                labels={'time_deviation': 'Time Deviation (minutes)', 'count': 'Frequency'},
                color_discrete_sequence=['#636EFA']
            )
            fig_hist.add_vline(x=deviation_threshold, line_dash="dash", line_color="red", 
                              annotation_text=f"Threshold: {deviation_threshold} min")
            st.plotly_chart(fig_hist, use_container_width=True)
        
        with col2:
            # Route type distribution
            route_dist = filtered_df['route_type'].value_counts()
            fig_pie = px.pie(
                values=route_dist.values, 
                names=route_dist.index,
                title="Distribution by Route Type",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        
        # Top problematic routes
        st.subheader("🚨 Routes Exceeding Deviation Threshold")
        problematic_routes = filtered_df[filtered_df['time_deviation'] > deviation_threshold]
        if not problematic_routes.empty:
            problem_summary = problematic_routes.groupby('route').agg({
                'time_deviation': ['count', 'mean', 'max'],
                'is_cutoff': 'sum'
            }).round(2)
            problem_summary.columns = ['Violation Count', 'Avg Deviation', 'Max Deviation', 'Cutoff Count']
            problem_summary = problem_summary.sort_values('Violation Count', ascending=False).head(10)
            st.dataframe(problem_summary, use_container_width=True)
        else:
            st.success("No routes exceed the current deviation threshold! 🎉")
    
    with tab2:
        # Route Analysis tab
        st.subheader("🗺️ Route Performance Analysis")
        
        # Route performance heatmap
        if 'route_performance' in summary_stats and not summary_stats['route_performance'].empty:
            route_perf = summary_stats['route_performance'].head(20)
            
            # Create a pivot table for heatmap
            heatmap_data = route_perf.pivot_table(
                index='source', 
                columns='destination', 
                values='avg_deviation', 
                fill_value=0
            )
            
            fig_heatmap = px.imshow(
                heatmap_data,
                title="Route Performance Heatmap (Average Time Deviation)",
                labels=dict(x="Destination", y="Source", color="Avg Deviation (min)"),
                aspect="auto",
                color_continuous_scale="RdYlBu_r"
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top routes by volume
            top_routes = filtered_df.groupby('route').agg({
                'trip_uuid': 'count',
                'time_deviation': 'mean',
                'is_cutoff': 'sum'
            }).round(2)
            top_routes.columns = ['Trip Count', 'Avg Deviation', 'Cutoff Count']
            top_routes = top_routes.sort_values('Trip Count', ascending=False).head(10)
            
            fig_routes = px.bar(
                x=top_routes.index,
                y=top_routes['Trip Count'],
                title="Top 10 Routes by Volume",
                labels={'x': 'Route', 'y': 'Trip Count'}
            )
            fig_routes.update_xaxes(tickangle=45)
            st.plotly_chart(fig_routes, use_container_width=True)
        
        with col2:
            # Efficiency scatter plot
            route_efficiency = filtered_df.groupby('route').agg({
                'efficiency_ratio': 'mean',
                'trip_uuid': 'count',
                'time_deviation': 'mean'
            }).reset_index()
            route_efficiency = route_efficiency[route_efficiency['trip_uuid'] >= 5]  # Filter routes with at least 5 trips
            
            fig_scatter = px.scatter(
                route_efficiency,
                x='efficiency_ratio',
                y='time_deviation',
                size='trip_uuid',
                hover_data=['route'],
                title="Route Efficiency vs Time Deviation",
                labels={
                    'efficiency_ratio': 'Efficiency Ratio',
                    'time_deviation': 'Avg Time Deviation (min)',
                    'trip_uuid': 'Trip Count'
                }
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
    
    with tab3:
        # Performance Trends tab
        st.subheader("📈 Performance Trends Over Time")
        
        if 'daily_stats' in summary_stats and not summary_stats['daily_stats'].empty:
            daily_stats = summary_stats['daily_stats']
            daily_stats['full_date'] = pd.to_datetime(daily_stats['full_date'])
            
            # Multi-metric time series
            fig_trends = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Daily Trip Volume', 'Average Time Deviation', 
                               'Cutoff Violations', 'Average Distance'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            # Trip volume
            fig_trends.add_trace(
                go.Scatter(x=daily_stats['full_date'], y=daily_stats['trip_count'],
                          name='Trip Count', line=dict(color='blue')),
                row=1, col=1
            )
            
            # Time deviation
            fig_trends.add_trace(
                go.Scatter(x=daily_stats['full_date'], y=daily_stats['avg_deviation'],
                          name='Avg Deviation', line=dict(color='red')),
                row=1, col=2
            )
            
            # Cutoff violations
            fig_trends.add_trace(
                go.Scatter(x=daily_stats['full_date'], y=daily_stats['cutoff_count'],
                          name='Cutoff Count', line=dict(color='orange')),
                row=2, col=1
            )
            
            # Average distance
            fig_trends.add_trace(
                go.Scatter(x=daily_stats['full_date'], y=daily_stats['avg_distance'],
                          name='Avg Distance', line=dict(color='green')),
                row=2, col=2
            )
            
            fig_trends.update_layout(height=600, showlegend=False, 
                                   title_text="Performance Trends Dashboard")
            st.plotly_chart(fig_trends, use_container_width=True)
        
        # Day of week analysis
        col1, col2 = st.columns(2)
        
        with col1:
            dow_analysis = filtered_df.groupby('day_of_week').agg({
                'trip_uuid': 'count',
                'time_deviation': 'mean',
                'is_cutoff': 'sum'
            }).round(2)
            dow_analysis.columns = ['Trip Count', 'Avg Deviation', 'Cutoff Count']
            
            # Reorder days
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            dow_analysis = dow_analysis.reindex([day for day in day_order if day in dow_analysis.index])
            
            fig_dow = px.bar(
                x=dow_analysis.index,
                y=dow_analysis['Trip Count'],
                title="Trip Volume by Day of Week",
                color=dow_analysis['Avg Deviation'],
                color_continuous_scale="RdYlBu_r"
            )
            st.plotly_chart(fig_dow, use_container_width=True)
        
        with col2:
            # Weekend vs Weekday comparison
            weekend_comparison = filtered_df.groupby('is_weekend').agg({
                'trip_uuid': 'count',
                'time_deviation': 'mean',
                'actual_time': 'mean',
                'is_cutoff': 'sum'
            }).round(2)
            weekend_comparison.index = ['Weekday', 'Weekend']
            
            st.subheader("Weekend vs Weekday Performance")
            st.dataframe(weekend_comparison)
    
    with tab4:
        # Operations tab
        st.subheader("🚛 Operational Insights")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Vehicle type analysis
            vehicle_analysis = filtered_df.groupby('vehicle_type').agg({
                'trip_uuid': 'count',
                'time_deviation': 'mean',
                'actual_distance_to_destination': 'mean'
            }).round(2)
            vehicle_analysis.columns = ['Trip Count', 'Avg Deviation', 'Avg Distance']
            
            st.subheader("Performance by Vehicle Type")
            st.dataframe(vehicle_analysis)
        
        with col2:
            # Segment factor analysis
            fig_segment = px.scatter(
                filtered_df.sample(min(1000, len(filtered_df))),  # Sample for performance
                x='segment_factor',
                y='time_deviation',
                color='route_type',
                title="Segment Factor vs Time Deviation",
                opacity=0.6
            )
            st.plotly_chart(fig_segment, use_container_width=True)
        
        # Operational alerts
        st.subheader("🚨 Operational Alerts")
        
        alerts = []
        
        # High deviation routes
        high_dev_routes = filtered_df.groupby('route')['time_deviation'].mean()
        critical_routes = high_dev_routes[high_dev_routes > deviation_threshold * 2].head(5)
        if not critical_routes.empty:
            alerts.append({
                'Type': 'Critical Delay',
                'Description': f"{len(critical_routes)} routes with extremely high deviations",
                'Action': 'Immediate investigation required'
            })
        
        # High cutoff rate
        cutoff_rate = (filtered_df['is_cutoff'].sum() / len(filtered_df)) * 100 if len(filtered_df) > 0 else 0
        if cutoff_rate > 10:
            alerts.append({
                'Type': 'High Cutoff Rate',
                'Description': f"Cutoff rate is {cutoff_rate:.1f}% (above 10% threshold)",
                'Action': 'Review capacity planning'
            })
        
        # Low efficiency routes
        low_eff_routes = filtered_df.groupby('route')['efficiency_ratio'].mean()
        inefficient_routes = low_eff_routes[low_eff_routes < 0.7].head(3)
        if not inefficient_routes.empty:
            alerts.append({
                'Type': 'Low Efficiency',
                'Description': f"{len(inefficient_routes)} routes operating below 70% efficiency",
                'Action': 'Route optimization needed'
            })
        
        if alerts:
            alerts_df = pd.DataFrame(alerts)
            st.dataframe(alerts_df, use_container_width=True)
        else:
            st.success("No critical alerts at this time! 🎉")
    
    with tab5:
        # Data Quality tab
        st.subheader("📋 Data Quality Assessment")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Missing values analysis
            missing_data = filtered_df.isnull().sum()
            missing_pct = (missing_data / len(filtered_df)) * 100
            
            quality_df = pd.DataFrame({
                'Column': missing_data.index,
                'Missing Count': missing_data.values,
                'Missing %': missing_pct.values
            })
            quality_df = quality_df[quality_df['Missing Count'] > 0].sort_values('Missing %', ascending=False)
            
            if not quality_df.empty:
                st.subheader("Missing Data Analysis")
                st.dataframe(quality_df)
            else:
                st.success("No missing data detected! ✅")
        
        with col2:
            # Data anomalies
            st.subheader("Data Anomalies Detected")
            
            anomalies = []
            
            # Negative time deviations (very unusual)
            neg_deviations = len(filtered_df[filtered_df['time_deviation'] < -60])  # More than 1 hour early
            if neg_deviations > 0:
                anomalies.append(f"{neg_deviations} trips arrived >1 hour early")
            
            # Extreme deviations
            extreme_deviations = len(filtered_df[filtered_df['time_deviation'] > 300])  # More than 5 hours late
            if extreme_deviations > 0:
                anomalies.append(f"{extreme_deviations} trips with >5 hour delays")
            
            # Zero distances
            zero_distances = len(filtered_df[filtered_df['actual_distance_to_destination'] == 0])
            if zero_distances > 0:
                anomalies.append(f"{zero_distances} trips with zero distance")
            
            # Same source and destination
            same_location = len(filtered_df[filtered_df['source_name'] == filtered_df['dest_name']])
            if same_location > 0:
                anomalies.append(f"{same_location} trips with same source and destination")
            
            if anomalies:
                for anomaly in anomalies:
                    st.warning(f"⚠️ {anomaly}")
            else:
                st.success("No significant anomalies detected! ✅")
        
        # Statistical summary
        st.subheader("Statistical Summary")
        numeric_cols = ['actual_time', 'osrm_time', 'time_deviation', 'actual_distance_to_destination', 'osrm_distance']
        summary_stats_df = filtered_df[numeric_cols].describe().round(2)
        st.dataframe(summary_stats_df)
    
    with tab6:
        # Deep Dive tab
        st.subheader("🔍 Deep Dive Analysis")
        
        # Interactive route explorer
        st.subheader("Route Explorer")
        
        selected_route_detailed = st.selectbox(
            "Select a route for detailed analysis:",
            options=[''] + sorted(filtered_df['route'].unique().tolist()),
            key="route_explorer"
        )
        
        if selected_route_detailed:
            route_data = filtered_df[filtered_df['route'] == selected_route_detailed]
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Trips", len(route_data))
                st.metric("Avg Deviation", f"{route_data['time_deviation'].mean():.1f} min")
            
            with col2:
                st.metric("Success Rate", f"{((len(route_data) - route_data['is_cutoff'].sum()) / len(route_data) * 100):.1f}%")
                st.metric("Avg Distance", f"{route_data['actual_distance_to_destination'].mean():.1f} km")
            
            with col3:
                st.metric("Best Performance", f"{route_data['time_deviation'].min():.1f} min")
                st.metric("Worst Performance", f"{route_data['time_deviation'].max():.1f} min")
            
            # Time series for selected route
            route_daily = route_data.groupby(route_data['full_date'].dt.date).agg({
                'time_deviation': 'mean',
                'trip_uuid': 'count'
            }).reset_index()
            
            fig_route_trend = px.line(
                route_daily,
                x='full_date',
                y='time_deviation',
                title=f"Time Deviation Trend for {selected_route_detailed}",
                hover_data=['trip_uuid']
            )
            st.plotly_chart(fig_route_trend, use_container_width=True)
            
            # Recent trips table
            st.subheader("Recent Trips")
            recent_trips = route_data.nlargest(10, 'full_date')[
                ['full_date', 'actual_time', 'osrm_time', 'time_deviation', 'is_cutoff']
            ]
            st.dataframe(recent_trips, use_container_width=True)
        
        # Custom query interface
        st.subheader("Custom Analysis")
        
        if st.checkbox("Enable Advanced Filtering"):
            col1, col2 = st.columns(2)
            
            with col1:
                min_distance = st.number_input("Minimum Distance (km)", value=0.0)
                max_distance = st.number_input("Maximum Distance (km)", value=float(filtered_df['actual_distance_to_destination'].max()))
            
            with col2:
                min_time = st.number_input("Minimum Trip Time (min)", value=0.0)
                max_time = st.number_input("Maximum Trip Time (min)", value=float(filtered_df['actual_time'].max()))
            
            # Apply advanced filters
            advanced_filtered = filtered_df[
                (filtered_df['actual_distance_to_destination'] >= min_distance) &
                (filtered_df['actual_distance_to_destination'] <= max_distance) &
                (filtered_df['actual_time'] >= min_time) &
                (filtered_df['actual_time'] <= max_time)
            ]
            
            st.write(f"Filtered to {len(advanced_filtered)} trips")
            
            if len(advanced_filtered) > 0:
                # Custom scatter plot
                fig_custom = px.scatter(
                    advanced_filtered.sample(min(500, len(advanced_filtered))),
                    x='actual_distance_to_destination',
                    y='actual_time',
                    color='time_deviation',
                    size='osrm_time',
                    hover_data=['route', 'is_cutoff'],
                    title="Custom Analysis: Distance vs Time",
                    color_continuous_scale="RdYlBu_r"
                )
                st.plotly_chart(fig_custom, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; font-size: 0.8rem;'>
        📊 Delhivery Logistics Dashboard | Last Updated: {} | Total Records: {:,}
    </div>
    """.format(datetime.now().strftime("%Y-%m-%d %H:%M"), len(df)), unsafe_allow_html=True)

if __name__ == "__main__":
    main()