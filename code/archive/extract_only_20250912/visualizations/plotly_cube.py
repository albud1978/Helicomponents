"""
Визуализация: каркасный 3D‑куб с осями на Plotly.

Назначение
— Построить куб размером size×size×size (по умолчанию 6) в диапазонах [0, size] для X/Y/Z.
— Сохранить интерактивный HTML с графиком.

Дата: 2025-08-29
"""

from __future__ import annotations

import argparse
import os
from typing import List, Tuple

import plotly.graph_objects as go


def _generate_cube_edges(size_x: int, size_y: int, size_z: int) -> Tuple[List[int], List[int], List[int]]:
    """
    Возвращает координаты рёбер куба (с разделителями None для отдельных сегментов).

    :param size: Длина ребра куба (используется как максимальное значение осей)
    :return: Кортеж списков X, Y, Z для Scatter3d(mode="lines")
    """
    # Вершины куба
    corners = [
        (0, 0, 0), (size_x, 0, 0), (size_x, size_y, 0), (0, size_y, 0),  # нижнее основание
        (0, 0, size_z), (size_x, 0, size_z), (size_x, size_y, size_z), (0, size_y, size_z),  # верхнее основание
    ]

    # Пары индексов вершин для рёбер (12 рёбер)
    edges = [
        (0, 1), (1, 2), (2, 3), (3, 0),  # низ
        (4, 5), (5, 6), (6, 7), (7, 4),  # верх
        (0, 4), (1, 5), (2, 6), (3, 7),  # стойки
    ]

    xs: List[int] = []
    ys: List[int] = []
    zs: List[int] = []
    for a, b in edges:
        xa, ya, za = corners[a]
        xb, yb, zb = corners[b]
        xs += [xa, xb, None]
        ys += [ya, yb, None]
        zs += [za, zb, None]

    return xs, ys, zs


def build_cube_figure(
    size_x: int = 8,
    size_y: int = 6,
    size_z: int = 6,
    eye: Tuple[float, float, float] | None = None,
    up: Tuple[float, float, float] | None = None,
    projection_type: str = "perspective",
    x_label_z_offset: float | None = None,
    x_label_x_shift: float | None = None,
    x_title_shift: float | None = None,
) -> go.Figure:
    """
    Создаёт фигуру Plotly с каркасом куба и осями 0..size (шаг 1).

    :param size_x: Размер по оси X (количество ячеек)
    :param size_y: Размер по оси Y (количество ячеек)
    :param size_z: Размер по оси Z (количество ячеек)
    :return: go.Figure
    """
    xs, ys, zs = _generate_cube_edges(size_x, size_y, size_z)

    cube_edges = go.Scatter3d(
        x=xs,
        y=ys,
        z=zs,
        mode="lines",
        line=dict(color="black", width=4),
        name="cube",
        showlegend=False,
    )

    if eye is None:
        # Ракурс: камера спереди‑справа, чуть выше; Z уходит «в глубину» справа внизу
        eye = (1.6, 1.2, -2.6)
    if up is None:
        up = (0.0, 1.0, 0.0)

    # Подписи как 3D‑аннотации (смещаем только текст, не оси)
    annotations = [
        dict(
            x=0.0,
            y=float(size_y) / 2.0,
            z=0.0,
            text="<b>Окружение</b>",
            showarrow=False,
            xanchor="right",
            yanchor="middle",
            font=dict(size=14, color="black"),
            xshift=-50,  # смещение влево на 10 px
        ),
    ]

    # Центры ячеек и границы для сетки
    x_centers = [i - 0.5 for i in range(1, size_x + 1)]
    x_grid = list(range(0, size_x + 1))
    y_centers = [i - 0.5 for i in range(1, size_y + 1)]
    y_grid = list(range(0, size_y + 1))
    z_centers = [i - 0.5 for i in range(1, size_z + 1)]
    z_grid = list(range(0, size_z + 1))
    y_tick_texts_grid = [str(v) for v in y_grid]

    fig = go.Figure(data=[cube_edges])
    # Подписи для оси X (Агенты)
    x_labels_source = [
        "sne",
        "ppr",
        "repair_days",
        "ops_ticket",
        "partout_trigger",
        "assembly_trigger",
    ]
    # Итоговый список для X из 8 значений по центрам 1..8
    x_tick_texts = [
        "psn",
        "sne",
        "ppr",
        "repair_days",
        "ops_ticket",
        "partout_trigger",
        "assembly_trigger",
        "active_trigger",
    ]
    y_tick_texts = [str(i) for i in range(1, size_y + 1)]
    z_tick_texts = [""] * size_z
    base_dates = ["04.07", "05.07", "06.07", "07.07", "08.07"]
    for i in range(min(5, size_z)):
        z_tick_texts[i] = base_dates[i]
    if size_z >= 6:
        z_tick_texts[5] = "…"
    for i in range(6, size_z):
        z_tick_texts[i] = "…"
    # Добавим подписи X как 3D‑аннотации, чтобы гарантировать отрисовку
    if x_label_z_offset is None:
        z_offset = 0.1
    else:
        z_offset = float(x_label_z_offset)
    if x_label_x_shift is None:
        x_shift = -20.0
    else:
        x_shift = float(x_label_x_shift)
    # Позиции подписей X: центры ячеек 1..size_x
    x_positions = x_centers
    for idx, label in enumerate(x_tick_texts[:size_x]):
        if not label:
            continue
        annotations.append(
            dict(
                x=x_positions[idx],
                y=0.0,
                z=z_offset,
                text=label,
                showarrow=False,
                xanchor="right",
                yanchor="top",
                font=dict(size=12, color="black"),
                xshift=x_shift,
            )
        )

    # Подписи оси Z (даты) по центрам ячеек, сетка остаётся по целым
    for zi, ztext in enumerate(z_tick_texts[:size_z]):
        if not ztext:
            continue
        annotations.append(
            dict(
                x=float(size_x),
                y=0.0,
                z=z_centers[zi],
                text=ztext,
                showarrow=False,
                xanchor="left",
                yanchor="middle",
                font=dict(size=10, color="black"),
                xshift=6,
            )
        )

    # Заголовок оси X как аннотация, смещённая от куба
    if x_title_shift is None:
        x_title_yshift = -20.0
    else:
        x_title_yshift = float(x_title_shift)
    annotations.append(
        dict(
            x=float(size_x) / 2.0,
            y=0.0,
            z=0.0,
            text="<b>Агенты</b>",
            showarrow=False,
            xanchor="center",
            yanchor="top",
            font=dict(size=14, color="black"),
            yshift=x_title_yshift,
        )
    )

    # Заголовок оси Z как аннотация, повернутая на 90° по часовой стрелке
    # Заголовок оси Z как аннотация, смещение вправо через xshift
    # Фиксированный сдвиг вправо от ребра
    z_xshift = 100.0

    annotations.append(
        dict(
            x=float(size_x),
            y=0.0,
            z=float(size_z) / 2.0,
            text="<b>Даты</b>",
            textangle=0,
            showarrow=False,
            xanchor="left",
            yanchor="middle",
            font=dict(size=14, color="black"),
            xshift=z_xshift,
        )
    )

    fig.update_layout(
        scene=dict(
            xaxis=dict(range=[0, size_x], tickmode="array", tickvals=x_grid, ticktext=[], showticklabels=False, title="", showgrid=True, zeroline=False, autorange="reversed"),
            yaxis=dict(range=[0, size_y], tickmode="array", tickvals=y_grid, ticktext=y_tick_texts_grid, title="", showgrid=True, zeroline=False),
            zaxis=dict(
                range=[0, size_z],
                tickmode="array",
                tickvals=z_grid,
                ticktext=[],
                showticklabels=False,
                title="",
                showgrid=True,
                zeroline=False,
            ),
            aspectmode="data",
            camera=dict(
                up=dict(x=up[0], y=up[1], z=up[2]),
                eye=dict(x=eye[0], y=eye[1], z=eye[2]),
                projection=dict(type=projection_type),
            ),
            annotations=annotations,
        ),
        margin=dict(l=0, r=0, t=30, b=0),
        title=f"3D куб {size_x}×{size_y}×{size_z}",
    )
    return fig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Визуализация каркасного 3D‑куба на Plotly")
    parser.add_argument("--size", type=int, default=6, help="Размер куба (длина ребра), по умолчанию 6")
    parser.add_argument(
        "--save",
        type=str,
        default=os.path.join("code", "outputs", "cube_6.html"),
        help="Путь для сохранения HTML (по умолчанию code/outputs/cube_6.html)",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="Автоматически открыть HTML после сохранения",
    )
    parser.add_argument("--eye-x", type=float, default=-1.6, help="Камера: eye.x (по умолчанию 2.6)")
    parser.add_argument("--eye-y", type=float, default=1.2, help="Камера: eye.y (по умолчанию 1.2)")
    parser.add_argument("--eye-z", type=float, default=-2.6, help="Камера: eye.z (по умолчанию -2.6)")
    parser.add_argument("--up-x", type=float, default=0.0, help="Вектор up.x (по умолчанию 0.0)")
    parser.add_argument("--up-y", type=float, default=1.0, help="Вектор up.y (по умолчанию 1.0)")
    parser.add_argument("--up-z", type=float, default=0.0, help="Вектор up.z (по умолчанию 0.0)")
    parser.add_argument("--projection", choices=["perspective", "orthographic"], default="perspective", help="Тип проекции камеры")
    parser.add_argument("--x-label-z-offset", type=float, default=0.0, help="Смещение подписей X по оси Z от переднего ребра")
    parser.add_argument("--x-label-x-shift", type=float, default=0.0, help="Смещение подписей X в пикселях влево/вправо")
    parser.add_argument("--x-title-shift", type=float, default=-60.0, help="Смещение заголовка оси X (Агенты) по вертикали, px")
    return parser.parse_args()


def main() -> None:
    # Жёстко заданные размеры сцен: X=8, Y=6, Z=6
    size_x: int = 8
    size_y: int = 8
    size_z: int = 8

    fig = build_cube_figure(
        size_x=size_x,
        size_y=size_y,
        size_z=size_z,
        eye=(-1.6, 1.2, -2.6),
        up=(0.0, 1.0, 0.0),
        projection_type="orthographic",
        x_label_z_offset=0.0,
        x_label_x_shift=0.0,
        x_title_shift=-60.0,
    )

    save_path = os.path.join("code", "outputs", "cube_8.html")
    # Создадим каталог для вывода, если требуется
    directory = os.path.dirname(save_path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    fig.write_html(save_path, include_plotlyjs="cdn", auto_open=True)


if __name__ == "__main__":
    main()


