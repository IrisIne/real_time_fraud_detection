from __future__ import annotations

import json
import os
from typing import Any
from uuid import uuid4

from confluent_kafka import Producer
import pandas as pd
import plotly.express as px
import psycopg
import streamlit as st


INPUT_COLUMNS = [
    "transaction_time",
    "merch",
    "cat_id",
    "amount",
    "name_1",
    "name_2",
    "gender",
    "street",
    "one_city",
    "us_state",
    "post_code",
    "lat",
    "lon",
    "population_city",
    "jobs",
    "merchant_lat",
    "merchant_lon",
]

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.98"))


def postgres_dsn() -> str:
    return (
        f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
        f"port={os.getenv('POSTGRES_PORT', '5432')} "
        f"dbname={os.getenv('POSTGRES_DB', 'fraud_detection')} "
        f"user={os.getenv('POSTGRES_USER', 'fraud_user')} "
        f"password={os.getenv('POSTGRES_PASSWORD', 'fraud_password')} "
        "connect_timeout=5"
    )


@st.cache_resource
def kafka_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def json_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def send_transactions(frame: pd.DataFrame) -> int:
    producer = kafka_producer()
    errors: list[str] = []

    def delivery_callback(error, _message) -> None:
        if error is not None:
            errors.append(str(error))

    progress = st.progress(0, text="Отправка транзакций в Kafka")
    for position, (_, row) in enumerate(frame.iterrows(), start=1):
        transaction_id = str(uuid4())
        data = {column: json_value(row[column]) for column in INPUT_COLUMNS}
        message = {"transaction_id": transaction_id, "data": data}
        producer.produce(
            TRANSACTIONS_TOPIC,
            key=transaction_id.encode("utf-8"),
            value=json.dumps(message, ensure_ascii=False, allow_nan=False).encode("utf-8"),
            on_delivery=delivery_callback,
        )
        producer.poll(0)
        progress.progress(position / len(frame), text=f"Отправлено {position} из {len(frame)}")

    remaining = producer.flush(15.0)
    progress.empty()
    if remaining:
        raise TimeoutError(f"Не доставлено сообщений: {remaining}")
    if errors:
        raise RuntimeError(errors[0])
    return len(frame)


def load_results() -> tuple[pd.DataFrame, pd.DataFrame]:
    with psycopg.connect(postgres_dsn()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT transaction_id::text, score, fraud_flag, created_at
                FROM transaction_scores
                WHERE fraud_flag = 1
                ORDER BY created_at DESC
                LIMIT 10
                """
            )
            fraud_rows = cursor.fetchall()
            cursor.execute(
                """
                SELECT transaction_id::text, score, fraud_flag, created_at
                FROM transaction_scores
                ORDER BY created_at DESC
                LIMIT 100
                """
            )
            score_rows = cursor.fetchall()

    fraud = pd.DataFrame(
        fraud_rows,
        columns=["transaction_id", "score", "fraud_flag", "created_at"],
    )
    scores = pd.DataFrame(
        score_rows,
        columns=["transaction_id", "score", "fraud_flag", "created_at"],
    )
    return fraud, scores


st.set_page_config(page_title="Скоринг транзакций", layout="centered")
st.markdown(
    """
    <style>
    [data-testid="stHeader"], [data-testid="stToolbar"], [data-testid="stMainMenu"], footer {
        display: none;
    }
    .block-container {
        max-width: 880px;
        padding-top: 1.25rem;
        padding-bottom: 1.25rem;
    }
    h1 {
        font-size: 1.85rem !important;
        margin-bottom: 0.1rem !important;
    }
    [data-testid="stCaptionContainer"] {
        margin-bottom: 0.25rem;
    }
    .metric-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 0.5rem;
        margin: 0.4rem 0 0.8rem;
    }
    .metric-card {
        background: #f5f6f7;
        border-radius: 0.4rem;
        padding: 0.5rem 0.65rem;
    }
    .metric-card span {
        color: #5f6368;
        display: block;
        font-size: 0.72rem;
        line-height: 1rem;
        white-space: nowrap;
    }
    .metric-card strong {
        display: block;
        font-size: 1.15rem;
        font-weight: 500;
        line-height: 1.5rem;
    }
    div[data-testid="stDataFrame"] {
        margin-bottom: 0.5rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("Скоринг транзакций")
st.caption("Загрузка транзакций, скоринг модели и результаты проверки")

send_tab, results_tab = st.tabs(["Загрузить данные", "Результаты"])

with send_tab:
    uploaded_file = st.file_uploader(".csv", type=["csv"])
    if uploaded_file is not None:
        try:
            source_frame = pd.read_csv(uploaded_file)
            missing_columns = [column for column in INPUT_COLUMNS if column not in source_frame.columns]
            if missing_columns:
                st.error("В файле отсутствуют столбцы: " + ", ".join(missing_columns))
            elif source_frame.empty:
                st.warning("Файл не содержит транзакций")
            else:
                controls = st.columns([1, 1])
                with controls[0]:
                    max_rows = st.number_input(
                        f"Количество строк из {len(source_frame)}",
                        min_value=1,
                        max_value=len(source_frame),
                        value=min(100, len(source_frame)),
                        step=1,
                    )
                with controls[1]:
                    st.write("")
                    send_button = st.button(
                        "Отправить на скоринг",
                        type="primary",
                        use_container_width=True,
                    )
                with st.expander("Предпросмотр первых 10 строк"):
                    st.dataframe(
                        source_frame.head(10),
                        use_container_width=True,
                        height=240,
                    )
                if send_button:
                    try:
                        sent = send_transactions(source_frame.iloc[: int(max_rows)])
                        st.success(f"Отправлено транзакций: {sent}")
                    except Exception as exc:
                        st.error(f"Не удалось отправить сообщения: {exc}")
        except Exception as exc:
            st.error(f"Не удалось прочитать CSV: {exc}")

with results_tab:
    show_results = st.button(
        "Посмотреть результаты",
        type="primary",
        use_container_width=True,
    )
    if show_results:
        try:
            with st.spinner("Чтение результатов из PostgreSQL..."):
                fraud_frame, scores_frame = load_results()

            st.markdown("**Подозрительные транзакции**")
            if fraud_frame.empty:
                st.info("Транзакций с флагом фрода пока нет.")
            else:
                visible_fraud = fraud_frame.rename(
                    columns={
                        "transaction_id": "ID",
                        "score": "Скор",
                        "fraud_flag": "Флаг",
                        "created_at": "Время",
                    }
                )
                st.dataframe(
                    visible_fraud,
                    use_container_width=True,
                    hide_index=True,
                    height=min(390, 38 + 35 * len(fraud_frame)),
                )

            if scores_frame.empty:
                st.info("В базе пока нет результатов скоринга.")
            else:
                average_score = float(scores_frame["score"].mean())
                median_score = float(scores_frame["score"].median())
                maximum_score = float(scores_frame["score"].max())
                fraud_count = int((scores_frame["score"] > FRAUD_THRESHOLD).sum())

                st.markdown(
                    f"""
                    <div class="metric-grid">
                        <div class="metric-card">
                            <span>Транзакций</span><strong>{len(scores_frame)}</strong>
                        </div>
                        <div class="metric-card">
                            <span>Средний скор</span><strong>{average_score:.3f}</strong>
                        </div>
                        <div class="metric-card">
                            <span>Макс. скор</span><strong>{maximum_score:.3f}</strong>
                        </div>
                        <div class="metric-card">
                            <span>Выше порога</span><strong>{fraud_count}</strong>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                st.markdown("**Распределение скоров**")
                figure = px.histogram(
                    scores_frame,
                    x="score",
                    nbins=20,
                    range_x=[0.0, 1.0],
                    labels={"score": "Скор"},
                    color_discrete_sequence=["#355C7D"],
                )
                figure.add_vline(
                    x=FRAUD_THRESHOLD,
                    line_dash="dash",
                    line_color="#A23E48",
                )
                figure.update_layout(
                    height=220,
                    showlegend=False,
                    bargap=0.05,
                    plot_bgcolor="#FFFFFF",
                    margin={"l": 5, "r": 5, "t": 5, "b": 5},
                )
                figure.update_xaxes(showgrid=False)
                figure.update_yaxes(title_text=None, gridcolor="#ECEFF1")
                st.plotly_chart(
                    figure,
                    use_container_width=True,
                    config={"displayModeBar": False},
                )
                st.caption(
                    f"На графике {len(scores_frame)} самых свежих записей. "
                    f"Порог `fraud_flag = 1` - {FRAUD_THRESHOLD:.2f}."
                )
                st.caption(
                    f"Средний скор {average_score:.3f}, медианный {median_score:.3f}."
                )
        except Exception as exc:
            st.error(f"Не удалось получить результаты: {exc}")
