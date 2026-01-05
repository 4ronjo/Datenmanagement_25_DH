from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

# Page config
st.set_page_config(
    page_title="Movie & Collaboration Insights",
    layout="wide",
    initial_sidebar_state="expanded",
)


BASE_DIR = Path(__file__).resolve().parent.parent
CURATED_DIR = BASE_DIR / "data" / "curated"


def _find_table(name: str) -> Tuple[Optional[Path], Optional[str]]:
    parquet_path = CURATED_DIR / f"{name}.parquet"
    csv_path = CURATED_DIR / f"{name}.csv"
    if parquet_path.exists():
        return parquet_path, "parquet"
    if csv_path.exists():
        return csv_path, "csv"
    return None, None


@st.cache_data(show_spinner="Loading curated tables...")
def load_curated_tables() -> Dict[str, pd.DataFrame]:
    tables: Dict[str, pd.DataFrame] = {}
    for name in [
        "curated_movie_overview",
        "curated_genre_stats",
        "curated_year_trends",
    ]:
        path, fmt = _find_table(name)
        if not path:
            tables[name] = pd.DataFrame()
            continue
        if fmt == "parquet":
            tables[name] = pd.read_parquet(path)
        else:
            tables[name] = pd.read_csv(path)
    # optional graph insights
    graph_path, graph_fmt = _find_table("graph_insights_top_coactors")
    if graph_path:
        tables["graph_insights"] = (
            pd.read_parquet(graph_path) if graph_fmt == "parquet" else pd.read_csv(graph_path)
        )
    else:
        tables["graph_insights"] = pd.DataFrame()
    return tables


def _ensure_columns(df: pd.DataFrame, required: List[str], optional: List[str]) -> List[str]:
    missing = [col for col in required if col not in df.columns]
    for col in missing + [c for c in optional if c not in df.columns]:
        df[col] = pd.NA
    return missing


def validate_tables(tables: Dict[str, pd.DataFrame]) -> None:
    movie_required = [
        "movie_id",
        "title",
        "release_year",
        "budget",
        "revenue",
        "profit",
        "roi",
    ]
    movie_optional = [
        "avg_rating",
        "rating_count",
        "vote_average",
        "genre_list",
        "keyword_list",
        "top_companies",
        "original_language",
    ]
    genre_required = ["genre_name", "movie_count", "avg_roi"]
    genre_optional = ["avg_rating"]
    year_required = ["release_year", "movie_count", "avg_budget", "avg_revenue"]
    year_optional = ["avg_rating"]

    missing_movies = _ensure_columns(
        tables["curated_movie_overview"], movie_required, movie_optional
    )
    missing_genres = _ensure_columns(
        tables["curated_genre_stats"], genre_required, genre_optional
    )
    missing_year = _ensure_columns(
        tables["curated_year_trends"], year_required, year_optional
    )

    if missing_movies:
        st.error(f"curated_movie_overview fehlt Spalten: {', '.join(missing_movies)}")
    if missing_genres:
        st.error(f"curated_genre_stats fehlt Spalten: {', '.join(missing_genres)}")
    if missing_year:
        st.error(f"curated_year_trends fehlt Spalten: {', '.join(missing_year)}")


def _split_genres(genre_str: str) -> List[str]:
    if pd.isna(genre_str):
        return []
    if isinstance(genre_str, list):
        return [g.strip() for g in genre_str if isinstance(g, str) and g.strip()]
    return [g.strip() for g in str(genre_str).split(",") if g.strip()]


def _rating_column(df: pd.DataFrame) -> str:
    """Pick the rating column with data; fall back to vote_average if avg_rating is empty."""
    if "avg_rating" in df.columns and df["avg_rating"].notna().any():
        return "avg_rating"
    if "vote_average" in df.columns and df["vote_average"].notna().any():
        return "vote_average"
    return "avg_rating" if "avg_rating" in df.columns else "vote_average"


def apply_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    filters: Dict[str, Any] = {}
    # Sidebar layout: keep the year slider on top
    year_slider_container = st.sidebar.empty()

    # Rating threshold first because it shrinks the available years
    if "rating_count" in df.columns and df["rating_count"].notna().any():
        max_rating = int(pd.to_numeric(df["rating_count"], errors="coerce").max())
        max_rating = max(max_rating, 0)
        default_min = 0  # be inclusive; early years often have few ratings
        filters["min_rating_count"] = st.sidebar.slider(
            "Minimum number of ratings", min_value=0, max_value=max_rating, value=default_min
        )
    else:
        filters["min_rating_count"] = None

    # Release year filter after applying the rating threshold
    filters["year_range"] = None
    if "release_year" in df.columns:
        year_source = df.copy()
        year_source["release_year"] = pd.to_numeric(
            year_source["release_year"], errors="coerce"
        )
        if filters["min_rating_count"] is not None and "rating_count" in year_source.columns:
            year_source = year_source[
                year_source["rating_count"].fillna(0) >= filters["min_rating_count"]
            ]
        years = year_source["release_year"].dropna().astype(int)
        if not years.empty:
            year_min = int(years.min())
            year_max = int(years.max())
            if year_min == year_max:
                filters["year_range"] = (year_min, year_max)
                with year_slider_container:
                    st.caption(f"Release Year: {year_min} (einziger Jahrgang nach Filter)")
            else:
                with year_slider_container:
                    filters["year_range"] = st.slider(
                        "Release Year Range",
                        min_value=year_min,
                        max_value=year_max,
                        value=(year_min, year_max),
                    )
    # Genres
    all_genres = sorted(
        {
            g
            for genres in df.get("genre_list", pd.Series(dtype=str)).dropna()
            for g in _split_genres(genres)
        }
    )
    filters["genres"] = st.sidebar.multiselect("Genres", options=all_genres)
    # Original language
    if "original_language" in df.columns:
        langs = sorted(df["original_language"].dropna().unique().tolist())
        filters["languages"] = st.sidebar.multiselect("Original Language", options=langs)
    else:
        filters["languages"] = []
    # Text search
    filters["search_text"] = st.sidebar.text_input("Titelsuche")

    df_filt = df.copy()
    if "release_year" in df_filt.columns:
        df_filt["release_year"] = pd.to_numeric(df_filt["release_year"], errors="coerce")
    if filters.get("year_range"):
        yr_min, yr_max = filters["year_range"]
        df_filt = df_filt[
            (df_filt["release_year"] >= yr_min) & (df_filt["release_year"] <= yr_max)
        ]
    if filters["genres"]:
        df_filt = df_filt[
            df_filt["genre_list"]
            .fillna("")
            .apply(lambda x: bool(set(_split_genres(x)) & set(filters["genres"])))
        ]
    if filters["languages"]:
        df_filt = df_filt[df_filt["original_language"].isin(filters["languages"])]
    if filters["min_rating_count"] is not None and "rating_count" in df_filt.columns:
        df_filt = df_filt[df_filt["rating_count"].fillna(0) >= filters["min_rating_count"]]
    if filters["search_text"]:
        needle = filters["search_text"].lower()
        df_filt = df_filt[df_filt["title"].str.lower().str.contains(needle, na=False)]
    return df_filt, filters


def kpi_overview(df: pd.DataFrame) -> None:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Filme (gefiltert)", f"{len(df):,}")
    rating_col = _rating_column(df)
    rating_value = (
        pd.to_numeric(df[rating_col], errors="coerce").mean() if rating_col in df.columns else None
    )
    with col2:
        st.metric(
            "Ø Rating",
            f"{rating_value:.2f}"
            if rating_value is not None and pd.notna(rating_value)
            else "n/a",
        )
    with col3:
        revenue_sum = (
            pd.to_numeric(df["revenue"], errors="coerce").sum()
            if "revenue" in df.columns
            else 0
        )
        st.metric("Revenue Sum", f"{revenue_sum:,.0f}")
    with col4:
        budget_median = pd.to_numeric(df["budget"], errors="coerce").median() if "budget" in df.columns else float("nan")
        st.metric(
            "Budget Median",
            f"{budget_median:,.0f}" if pd.notna(budget_median) else "n/a",
        )


def chart_top_genres(df: pd.DataFrame, genre_stats: pd.DataFrame) -> None:
    if not df.empty and "genre_list" in df.columns:
        exploded = (
            df["genre_list"]
            .dropna()
            .apply(_split_genres)
            .explode()
            .dropna()
        )
        if not exploded.empty:
            counts = exploded.value_counts().head(10).reset_index()
            counts.columns = ["genre_name", "movie_count"]
            fig = px.bar(counts, x="genre_name", y="movie_count", title="Top Genres (gefiltert)")
            st.plotly_chart(fig, use_container_width=True)
            return
    if not genre_stats.empty:
        top10 = genre_stats.sort_values("movie_count", ascending=False).head(10)
        fig = px.bar(top10, x="genre_name", y="movie_count", title="Top Genres (gesamt)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Keine Genre-Daten verfügbar.")


def chart_rating_hist(df: pd.DataFrame) -> None:
    rating_col = _rating_column(df)
    if rating_col in df.columns and df[rating_col].notna().any():
        df_plot = df.copy()
        df_plot[rating_col] = pd.to_numeric(df_plot[rating_col], errors="coerce")
        fig = px.histogram(df_plot, x=rating_col, nbins=30, title=f"Verteilung {rating_col}")
        st.plotly_chart(fig, use_container_width=True)
    elif "roi" in df.columns and df["roi"].notna().any():
        df_plot = df.copy()
        df_plot["roi"] = pd.to_numeric(df_plot["roi"], errors="coerce")
        fig = px.histogram(df_plot, x="roi", nbins=30, title="Verteilung ROI")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Keine Ratings/ROI für Histogramm verfügbar.")


def table_top_movies(df: pd.DataFrame) -> None:
    if df.empty:
        st.warning("Keine Filme nach Filterkriterien.")
        return
    rating_col = _rating_column(df)
    sort_cols = [rating_col] if rating_col in df.columns else []
    if "rating_count" in df.columns:
        sort_cols.append("rating_count")
    table_cols = [
        col
        for col in [
            "title",
            "release_year",
            rating_col,
            "rating_count",
            "revenue",
            "budget",
            "roi",
            "genre_list",
        ]
        if col in df.columns
    ]
    df_disp = df[table_cols]
    if sort_cols:
        df_disp = df_disp.sort_values(sort_cols, ascending=False)
    df_disp = df_disp.head(50)
    st.dataframe(df_disp, use_container_width=True)


def page_overview(df_filt: pd.DataFrame, genre_stats: pd.DataFrame) -> None:
    st.header("Overview")
    st.write("KPIs und Top-Genres/Filme basierend auf den gefilterten Daten.")
    kpi_overview(df_filt)
    col1, col2 = st.columns(2)
    with col1:
        chart_top_genres(df_filt, genre_stats)
    with col2:
        chart_rating_hist(df_filt)
    st.subheader("Top Movies")
    table_top_movies(df_filt)


def page_trends(year_trends: pd.DataFrame, filters: Dict[str, Any]) -> None:
    st.header("Trends")
    st.write("Zeitverlauf: Anzahl Filme, Revenue und optional Ratings.")
    if year_trends.empty:
        st.warning("Keine Year-Trend-Daten gefunden.")
        return
    yr = year_trends.copy()
    yr["release_year"] = pd.to_numeric(yr["release_year"], errors="coerce")
    yr = yr.dropna(subset=["release_year"])
    for col in ["movie_count", "avg_budget", "avg_revenue", "avg_rating"]:
        if col in yr.columns:
            yr[col] = pd.to_numeric(yr[col], errors="coerce")
    if filters.get("year_range"):
        yr_min, yr_max = filters["year_range"]
        yr = yr[(yr["release_year"] >= yr_min) & (yr["release_year"] <= yr_max)]
    if yr.empty:
        st.warning("Keine Daten im gewählten Jahr-Bereich.")
        return
    col1, col2 = st.columns(2)
    with col1:
        fig = px.line(yr, x="release_year", y="movie_count", title="Movie Count per Year")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.line(yr, x="release_year", y="avg_revenue", title="Avg Revenue per Year")
        st.plotly_chart(fig, use_container_width=True)
    if "avg_rating" in yr.columns and yr["avg_rating"].notna().any():
        fig = px.line(yr, x="release_year", y="avg_rating", title="Avg Rating per Year")
        st.plotly_chart(fig, use_container_width=True)


def page_roi(df_filt: pd.DataFrame, genre_stats: pd.DataFrame) -> None:
    st.header("ROI & Success")
    st.write("Budget, Revenue und ROI Analysen.")
    if df_filt.empty:
        st.warning("Keine Filme nach Filterkriterien.")
        return
    clean = df_filt.copy()
    for col in ["budget", "revenue", "roi"]:
        if col in clean.columns:
            clean[col] = pd.to_numeric(clean[col], errors="coerce")
    if {"budget", "revenue"}.issubset(clean.columns):
        clean = clean[(clean["budget"] > 0) & (clean["revenue"] > 0)]
    col1, col2 = st.columns(2)
    with col1:
        if not clean.empty:
            hover_cols = [
                c for c in ["title", "release_year", "genre_list", "avg_rating"] if c in clean.columns
            ]
            fig = px.scatter(
                clean,
                x="budget",
                y="revenue",
                hover_data=hover_cols,
                title="Budget vs Revenue",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Keine Daten für Budget vs Revenue.")
    with col2:
        if not genre_stats.empty and "avg_roi" in genre_stats.columns:
            top = genre_stats.sort_values("avg_roi", ascending=False).head(15)
            fig = px.bar(top, x="genre_name", y="avg_roi", title="Avg ROI nach Genre")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Keine Genre-ROI-Daten verfügbar.")
    st.subheader("ROI Verteilung")
    if "roi" in clean.columns and clean["roi"].notna().any():
        fig = px.histogram(clean, x="roi", nbins=40, title="ROI Histogramm")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Keine ROI-Daten verfügbar.")
    st.caption("ROI wird als revenue / budget berechnet; bei budget=0 -> ROI = NaN.")


def page_collab(graph_df: pd.DataFrame) -> None:
    st.header("Collaboration Graph Insights")
    st.write("Top Co-Actor Paare als Proxy für Graph-Erkenntnisse.")
    if graph_df.empty:
        st.warning(
            "Graph Insights Datei nicht gefunden. Bitte zuerst Neo4j Queries exportieren (graph_insights_top_coactors)."
        )
        return
    required = ["actor_1", "actor_2", "shared_movies_count"]
    missing = [c for c in required if c not in graph_df.columns]
    for col in missing:
        graph_df[col] = pd.NA
    if missing:
        st.warning(f"Graph Insights fehlen Spalten: {', '.join(missing)}")
    st.metric("Co-Actor Paare", f"{len(graph_df):,}")
    top_pairs = graph_df.sort_values("shared_movies_count", ascending=False).head(20)
    st.dataframe(top_pairs, use_container_width=True)
    if not top_pairs.empty:
        fig = px.bar(
            top_pairs.head(10),
            x="shared_movies_count",
            y="actor_1",
            color="actor_2",
            orientation="h",
            title="Top 10 Co-Actor Paare",
        )
        st.plotly_chart(fig, use_container_width=True)
    actors = sorted(
        {
            a
            for a in pd.concat(
                [graph_df["actor_1"].dropna(), graph_df["actor_2"].dropna()], ignore_index=True
            ).unique()
            if isinstance(a, str)
        }
    )
    if actors:
        selected = st.selectbox("Actor auswählen", options=["(kein Filter)"] + actors)
        if selected and selected != "(kein Filter)":
            subset = graph_df[
                (graph_df["actor_1"] == selected) | (graph_df["actor_2"] == selected)
            ].sort_values("shared_movies_count", ascending=False)
            st.subheader(f"Top Collaborations für {selected}")
            st.dataframe(subset.head(20), use_container_width=True)


def main() -> None:
    st.title("Movie & Collaboration Insights")
    st.sidebar.header("Navigation & Filter")

    tables = load_curated_tables()
    validate_tables(tables)

    movie_df = tables["curated_movie_overview"]
    genre_stats = tables["curated_genre_stats"]
    year_trends = tables["curated_year_trends"]
    graph_df = tables.get("graph_insights", pd.DataFrame())

    if movie_df.empty:
        st.error("Keine curated_movie_overview Daten gefunden.")
        return

    df_filt, filters = apply_filters(movie_df)

    page = st.sidebar.radio(
        "Seite",
        options=[
            "Overview",
            "Trends",
            "ROI & Success",
            "Collaboration Graph Insights",
        ],
    )

    if page == "Overview":
        page_overview(df_filt, genre_stats)
    elif page == "Trends":
        page_trends(year_trends, filters)
    elif page == "ROI & Success":
        page_roi(df_filt, genre_stats)
    elif page == "Collaboration Graph Insights":
        page_collab(graph_df)

    st.sidebar.markdown("---")
    csv_data = df_filt.to_csv(index=False).encode("utf-8")
    st.sidebar.download_button(
        "Download filtered movies as CSV",
        data=csv_data,
        file_name="filtered_movies.csv",
        mime="text/csv",
    )
    st.sidebar.caption("Hinweis: Datenbasis = Kaggle 'The Movies Dataset' mit ratings_small/links_small (MovieLens subset).")


if __name__ == "__main__":
    main()
