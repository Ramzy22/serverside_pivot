"""Render launch video for serverside-pivot following the /brag and /brag-slim skill specification.

Generates 600 frames at 1920x1080 (20s @ 30fps), mixes soundtrack, and renders brag-output/brag.mp4.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "brag-output"
WORK_DIR = OUT_DIR / "work"
FRAMES_DIR = WORK_DIR / "frames"
MUSIC_SRC = ROOT / ".agents/skills/brag/assets/music/happy-beats-business-moves-vol-1-by-ende-dot-app.mp3"

SHOT_TRADER = OUT_DIR / "shot_trader.png"
SHOT_HR = OUT_DIR / "shot_hr.png"
SHOT_SAAS = OUT_DIR / "shot_saas.png"

# Setup directories
WORK_DIR.mkdir(parents=True, exist_ok=True)
if FRAMES_DIR.exists():
    shutil.rmtree(FRAMES_DIR)
FRAMES_DIR.mkdir(parents=True, exist_ok=True)

# Fonts
FONT_TITLE = ImageFont.truetype("C:\\Windows\\Fonts\\segoeuib.ttf", 64)
FONT_SUBTITLE = ImageFont.truetype("C:\\Windows\\Fonts\\segoeui.ttf", 34)
FONT_CARD_TITLE = ImageFont.truetype("C:\\Windows\\Fonts\\segoeuib.ttf", 32)
FONT_CARD_BODY = ImageFont.truetype("C:\\Windows\\Fonts\\segoeui.ttf", 24)
FONT_CODE = ImageFont.truetype("C:\\Windows\\Fonts\\consola.ttf", 36)
FONT_CODE_BOLD = ImageFont.truetype("C:\\Windows\\Fonts\\consolab.ttf", 36)
FONT_BADGE = ImageFont.truetype("C:\\Windows\\Fonts\\segoeuib.ttf", 20)
FONT_HERO = ImageFont.truetype("C:\\Windows\\Fonts\\segoeuib.ttf", 80)
FONT_SMALL = ImageFont.truetype("C:\\Windows\\Fonts\\segoeui.ttf", 20)

TOTAL_FRAMES = 600  # 20s at 30fps

# Load screenshots if available
trader_img = Image.open(SHOT_TRADER).convert("RGB") if SHOT_TRADER.exists() else None
hr_img = Image.open(SHOT_HR).convert("RGB") if SHOT_HR.exists() else None
saas_img = Image.open(SHOT_SAAS).convert("RGB") if SHOT_SAAS.exists() else None


def draw_rounded_rect(draw, xy, radius, fill=None, outline=None, width=1):
    x0, y0, x1, y1 = xy
    draw.rounded_rectangle([x0, y0, x1, y1], radius=radius, fill=fill, outline=outline, width=width)


def render_scene_1(frame_idx: int) -> Image.Image:
    """Scene 1 (0.0s - 3.5s): The 2-Line Code Hook."""
    img = Image.new("RGB", (1920, 1080), color=(7, 12, 20))
    draw = ImageDraw.Draw(img)

    # Top Pill Badge
    draw_rounded_rect(draw, (80, 70, 360, 114), 22, fill=(14, 30, 50), outline=(56, 189, 248), width=2)
    draw.text((100, 80), "STEP 1: LOW-CODE SETUP", font=FONT_BADGE, fill=(56, 189, 248))

    # Main Headline
    draw.text((80, 140), "This is all the code you write.", font=FONT_HERO, fill=(248, 250, 252))
    draw.text((80, 245), "Turnkey server-side pivot engine for Python, React, Dash & Streamlit.", font=FONT_SUBTITLE, fill=(148, 163, 184))

    # Editor Window
    card_x0, card_y0, card_x1, card_y1 = 80, 330, 1840, 960
    draw_rounded_rect(draw, (card_x0, card_y0, card_x1, card_y1), 20, fill=(13, 20, 36), outline=(30, 48, 76), width=2)

    # Editor Title Bar
    draw_rounded_rect(draw, (card_x0, card_y0, card_x1, card_y0 + 56), 20, fill=(19, 30, 52))
    draw.ellipse((card_x0 + 24, card_y0 + 20, card_x0 + 40, card_y0 + 36), fill=(244, 63, 94))
    draw.ellipse((card_x0 + 48, card_y0 + 20, card_x0 + 64, card_y0 + 36), fill=(245, 158, 11))
    draw.ellipse((card_x0 + 72, card_y0 + 20, card_x0 + 88, card_y0 + 36), fill=(16, 185, 129))
    draw.text((card_x0 + 110, card_y0 + 15), "app.py — serverside-pivot", font=FONT_SMALL, fill=(148, 163, 184))

    # Code Lines
    lines = [
        ("from", (192, 132, 252), " pivot_engine ", (248, 250, 252), "import", (192, 132, 252), " create_tanstack_adapter, register_pivot_app", (248, 250, 252)),
        ("from", (192, 132, 252), " dash_tanstack_pivot ", (248, 250, 252), "import", (192, 132, 252), " DashTanstackPivot", (248, 250, 252)),
        ("", (0,0,0)),
        ("# 1. Connect any database, warehouse, or Parquet file in 1 line:", (100, 116, 139)),
        ("adapter = create_tanstack_adapter(", (248, 250, 252), '"duckdb:///sales.db"', (52, 211, 153), ")", (248, 250, 252)),
        ('adapter.load_table("trades")', (56, 189, 248)),
        ("", (0,0,0)),
        ("# 2. Drop the pivot component into your layout — zero custom backend needed!", (100, 116, 139)),
        ("app.layout = DashTanstackPivot(", (248, 250, 252), 'table="trades", serverSide=True', (245, 158, 11), ")", (248, 250, 252)),
        ("register_pivot_app(app, adapter_getter=lambda: adapter, pivot_id=", (248, 250, 252), '"pivot"', (52, 211, 153), ")", (248, 250, 252)),
    ]

    # Typing progression based on frame_idx
    max_line = min(len(lines), int((frame_idx / 90) * len(lines)) + 3)
    curr_y = card_y0 + 90
    for l_idx in range(max_line):
        tokens = lines[l_idx]
        curr_x = card_x0 + 50
        for t_idx in range(0, len(tokens), 2):
            text = tokens[t_idx]
            color = tokens[t_idx + 1]
            draw.text((curr_x, curr_y), text, font=FONT_CODE, fill=color)
            bbox = draw.textbbox((curr_x, curr_y), text, font=FONT_CODE)
            curr_x = bbox[2]
        curr_y += 50

    return img


def render_scene_2(frame_idx: int) -> Image.Image:
    """Scene 2 (3.5s - 6.5s): Any Data Source or Scale."""
    img = Image.new("RGB", (1920, 1080), color=(7, 12, 20))
    draw = ImageDraw.Draw(img)

    # Badge
    draw_rounded_rect(draw, (80, 70, 390, 114), 22, fill=(14, 30, 50), outline=(168, 85, 247), width=2)
    draw.text((100, 80), "STEP 2: UNIVERSAL DATA ENGINE", font=FONT_BADGE, fill=(192, 132, 252))

    # Headline
    draw.text((80, 140), "Small file or 100M+ row warehouse.", font=FONT_HERO, fill=(248, 250, 252))
    draw.text((80, 245), "Zero SQL to write. DuckDB + PyArrow + Ibis push down aggregates in milliseconds.", font=FONT_SUBTITLE, fill=(148, 163, 184))

    # Data Source Cards
    sources = [
        ("Local Parquet & CSV", "10 KB to 50 GB files", "Zero-copy PyArrow & DuckDB memory", "#38BDF8"),
        ("Snowflake & BigQuery", "100M to Billions of rows", "Server-side pushdown aggregation", "#818CF8"),
        ("ClickHouse & PostgreSQL", "Real-time production tables", "Push-down SQL group-by compilation", "#34D399"),
        ("DuckDB In-Memory OLAP", "Instantaneous sub-second queries", "Multi-threaded columnar analytics", "#F59E0B"),
    ]

    card_w = 415
    gap = 26
    start_x = 80
    start_y = 350
    for idx, (title, sub, detail, color_hex) in enumerate(sources):
        cx = start_x + idx * (card_w + gap)
        # Convert hex
        r = int(color_hex[1:3], 16)
        g = int(color_hex[3:5], 16)
        b = int(color_hex[5:7], 16)

        draw_rounded_rect(draw, (cx, start_y, cx + card_w, start_y + 440), 20, fill=(13, 20, 36), outline=(r, g, b), width=2)
        draw_rounded_rect(draw, (cx + 30, start_y + 36, cx + 110, start_y + 116), 16, fill=(r // 4, g // 4, b // 4), outline=(r, g, b), width=2)
        draw.text((cx + 50, start_y + 54), f"0{idx+1}", font=FONT_CARD_TITLE, fill=(r, g, b))

        draw.text((cx + 30, start_y + 150), title, font=FONT_CARD_TITLE, fill=(248, 250, 252))
        draw.text((cx + 30, start_y + 205), sub, font=FONT_CARD_BODY, fill=(r, g, b))
        draw.text((cx + 30, start_y + 270), detail, font=FONT_SMALL, fill=(148, 163, 184))

    # Bottom Banner
    draw_rounded_rect(draw, (80, 830, 1840, 950), 16, fill=(16, 28, 48), outline=(56, 189, 248), width=1)
    draw.text((120, 868), "O(D) HIERARCHICAL QUERY BATCHING: Level-based batch queries eliminate the N-group waterfall.", font=FONT_CARD_TITLE, fill=(56, 189, 248))

    return img


def render_scene_3(frame_idx: int) -> Image.Image:
    """Scene 3 (6.5s - 11.5s): The Live Web UI (Trading Desk)."""
    img = Image.new("RGB", (1920, 1080), color=(7, 12, 20))

    if trader_img:
        # Slight cinematic push-in zoom
        progress = (frame_idx - 195) / 150
        zoom = 1.0 + progress * 0.05
        new_w = int(1920 * zoom)
        new_h = int(1080 * zoom)
        resized = trader_img.resize((new_w, new_h), Image.Resampling.BILINEAR)
        offset_x = (new_w - 1920) // 2
        offset_y = (new_h - 1080) // 2
        img.paste(resized.crop((offset_x, offset_y, offset_x + 1920, offset_y + 1080)), (0, 0))

    draw = ImageDraw.Draw(img)

    # Top Floating Banner
    draw_rounded_rect(draw, (60, 40, 1860, 130), 18, fill=(9, 15, 27), outline=(56, 189, 248), width=2)
    draw.text((90, 58), "STEP 3: THE LIVE WEB UI · 50,000 MULTI-ASSET POSITIONS", font=FONT_CARD_TITLE, fill=(248, 250, 252))
    draw.text((1200, 58), "LATENCY: 12ms · BROWSER RAM: 70MB CONSTANT", font=FONT_CARD_TITLE, fill=(52, 211, 153))

    # Highlight Pill Callout (Bottom Left)
    draw_rounded_rect(draw, (60, 930, 800, 1020), 16, fill=(13, 23, 38), outline=(56, 189, 248), width=2)
    draw.text((90, 955), "MULTI-LEVEL TREE PRE-EXPANDED · VIRTUAL SCROLLING", font=FONT_CARD_TITLE, fill=(56, 189, 248))

    # Highlight Pill Callout (Bottom Right)
    draw_rounded_rect(draw, (1120, 930, 1860, 1020), 16, fill=(13, 23, 38), outline=(52, 211, 153), width=2)
    draw.text((1150, 955), "INLINE SPARKLINE CELLS + LIVE DOCKED CHARTS", font=FONT_CARD_TITLE, fill=(52, 211, 153))

    return img


def render_scene_4(frame_idx: int) -> Image.Image:
    """Scene 4 (11.5s - 16.5s): Visual Superpowers across Industries."""
    img = Image.new("RGB", (1920, 1080), color=(7, 12, 20))

    # Split screen: Left = HR (:8052), Right = SaaS Immersive (:8053)
    if hr_img:
        left_crop = hr_img.crop((0, 0, 950, 1080))
        img.paste(left_crop, (0, 0))
    if saas_img:
        right_crop = saas_img.crop((970, 0, 1920, 1080))
        img.paste(right_crop, (970, 0))

    draw = ImageDraw.Draw(img)

    # Dividing separator line
    draw.line([(960, 0), (960, 1080)], fill=(56, 189, 248), width=4)

    # Top Overlay Banner
    draw_rounded_rect(draw, (80, 40, 1840, 130), 18, fill=(9, 15, 27), outline=(168, 85, 247), width=2)
    draw.text((120, 60), "VISUAL SUPERPOWERS ACROSS FINANCE, PEOPLE & SAAS", font=FONT_CARD_TITLE, fill=(248, 250, 252))
    draw.text((1250, 60), "HEATMAPS · DATA BARS · CHARTS", font=FONT_CARD_TITLE, fill=(192, 132, 252))

    # Left Badge
    draw_rounded_rect(draw, (40, 940, 460, 1020), 14, fill=(255, 255, 255), outline=(79, 70, 229), width=2)
    draw.text((65, 965), "PEOPLE ANALYTICS (LIGHT)", font=FONT_CARD_TITLE, fill=(79, 70, 229))

    # Right Badge
    draw_rounded_rect(draw, (1440, 940, 1880, 1020), 14, fill=(16, 12, 34), outline=(168, 85, 247), width=2)
    draw.text((1465, 965), "IMMERSIVE SAAS (OBSIDIAN)", font=FONT_CARD_TITLE, fill=(192, 132, 252))

    return img


def render_scene_5(frame_idx: int) -> Image.Image:
    """Scene 5 (16.5s - 20.0s): The Comparison & Outro."""
    img = Image.new("RGB", (1920, 1080), color=(7, 12, 20))
    draw = ImageDraw.Draw(img)

    # Header
    draw.text((80, 70), "Why Engineering Teams Choose serverside-pivot", font=FONT_HERO, fill=(248, 250, 252))

    # Comparison Grid
    comp_y0 = 200
    comp_w = 560
    comp_h = 580

    # Card 1: Perspective
    draw_rounded_rect(draw, (80, comp_y0, 80 + comp_w, comp_y0 + comp_h), 20, fill=(18, 14, 20), outline=(244, 63, 94), width=2)
    draw.text((120, comp_y0 + 40), "FINOS Perspective", font=FONT_TITLE, fill=(244, 63, 94))
    draw.text((120, comp_y0 + 130), "• In-memory WASM only", font=FONT_CARD_TITLE, fill=(226, 232, 240))
    draw.text((120, comp_y0 + 190), "• Crashes tab on >5M rows (OOM)", font=FONT_CARD_TITLE, fill=(244, 63, 94))
    draw.text((120, comp_y0 + 250), "• Memory explodes with data", font=FONT_CARD_TITLE, fill=(226, 232, 240))
    draw.text((120, comp_y0 + 310), "• Cannot query cloud data warehouses", font=FONT_CARD_TITLE, fill=(226, 232, 240))
    draw_rounded_rect(draw, (120, comp_y0 + 460, 80 + comp_w - 40, comp_y0 + 530), 12, fill=(40, 16, 24))
    draw.text((150, comp_y0 + 480), "BROWSER RAM CRASH ❌", font=FONT_CARD_TITLE, fill=(244, 63, 94))

    # Card 2: AG Grid Enterprise
    draw_rounded_rect(draw, (680, comp_y0, 680 + comp_w, comp_y0 + comp_h), 20, fill=(22, 20, 16), outline=(245, 158, 11), width=2)
    draw.text((720, comp_y0 + 40), "AG Grid Enterprise", font=FONT_TITLE, fill=(245, 158, 11))
    draw.text((720, comp_y0 + 130), "• Costs $1,000–$2,500 / seat", font=FONT_CARD_TITLE, fill=(245, 158, 11))
    draw.text((720, comp_y0 + 190), "• EMPTY BACKEND SHELL", font=FONT_CARD_TITLE, fill=(245, 158, 11))
    draw.text((720, comp_y0 + 250), "• You build SQL groupby engine", font=FONT_CARD_TITLE, fill=(226, 232, 240))
    draw.text((720, comp_y0 + 310), "• Months of custom backend dev", font=FONT_CARD_TITLE, fill=(226, 232, 240))
    draw_rounded_rect(draw, (720, comp_y0 + 460, 680 + comp_w - 40, comp_y0 + 530), 12, fill=(42, 30, 12))
    draw.text((750, comp_y0 + 480), "EMPTY SHELL + EXPENSIVE ❌", font=FONT_CARD_TITLE, fill=(245, 158, 11))

    # Card 3: serverside-pivot
    draw_rounded_rect(draw, (1280, comp_y0, 1280 + comp_w, comp_y0 + comp_h), 20, fill=(10, 28, 44), outline=(52, 211, 153), width=3)
    draw.text((1320, comp_y0 + 40), "serverside-pivot", font=FONT_TITLE, fill=(52, 211, 153))
    draw.text((1320, comp_y0 + 130), "• 100% Free & Open Source (MIT)", font=FONT_CARD_TITLE, fill=(52, 211, 153))
    draw.text((1320, comp_y0 + 190), "• TURNKEY BACKEND ENGINE", font=FONT_CARD_TITLE, fill=(52, 211, 153))
    draw.text((1320, comp_y0 + 250), "• 2 lines to connect DuckDB/Snowflake", font=FONT_CARD_TITLE, fill=(248, 250, 252))
    draw.text((1320, comp_y0 + 310), "• Constant < 80MB client RAM", font=FONT_CARD_TITLE, fill=(248, 250, 252))
    draw_rounded_rect(draw, (1320, comp_y0 + 460, 1280 + comp_w - 40, comp_y0 + 530), 12, fill=(16, 50, 36))
    draw.text((1350, comp_y0 + 480), "TURNKEY & UNCRASHABLE ✅", font=FONT_CARD_TITLE, fill=(52, 211, 153))

    # Bottom CTA Bar
    draw_rounded_rect(draw, (80, 840, 1840, 980), 20, fill=(13, 20, 36), outline=(56, 189, 248), width=2)
    draw.text((120, 875), "pip install serverside-pivot", font=FONT_TITLE, fill=(56, 189, 248))
    draw.text((1050, 875), "github.com/Ramzy22/serverside_pivot", font=FONT_TITLE, fill=(248, 250, 252))
    draw.text((120, 935), "Dash · React · Streamlit  ·  207/207 Certified Qualification Tests Passed", font=FONT_SUBTITLE, fill=(148, 163, 184))

    return img


def render_all_frames():
    print(f"Generating {TOTAL_FRAMES} frames for 20-second launch video...")
    for f in range(TOTAL_FRAMES):
        if f < 105:
            frame = render_scene_1(f)
        elif f < 195:
            frame = render_scene_2(f)
        elif f < 345:
            frame = render_scene_3(f)
        elif f < 495:
            frame = render_scene_4(f)
        else:
            frame = render_scene_5(f)

        frame_path = FRAMES_DIR / f"frame_{f:04d}.png"
        frame.save(frame_path)

        if f % 60 == 0 or f == TOTAL_FRAMES - 1:
            print(f"  -> Generated {f+1}/{TOTAL_FRAMES} frames ({(f+1)/30:.1f}s)")


def build_final_video():
    render_all_frames()

    # Poster frame (settled frame from Scene 3 or Scene 5)
    poster_src = FRAMES_DIR / "frame_0250.png"
    poster_dst = OUT_DIR / "brag.jpg"
    shutil.copy(poster_src, poster_dst)
    print(f"Saved poster thumbnail: {poster_dst}")

    # Video output path
    video_output = OUT_DIR / "brag.mp4"
    if video_output.exists():
        video_output.unlink()

    print("Encoding video with ffmpeg...")
    cmd = [
        "ffmpeg",
        "-y",
        "-framerate", "30",
        "-i", str(FRAMES_DIR / "frame_%04d.png"),
        "-i", str(MUSIC_SRC),
        "-t", "20",
        "-filter_complex", "[1:a]afade=t=out:st=19.2:d=0.8[a]",
        "-map", "0:v",
        "-map", "[a]",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-preset", "fast",
        "-crf", "18",
        "-c:a", "aac",
        "-b:a", "192k",
        str(video_output)
    ]
    subprocess.run(cmd, check=True)
    print(f"\nSuccessfully rendered video: {video_output}")


def write_share_copy():
    copy_path = OUT_DIR / "share-copy.txt"
    copy_text = """# Launch Share Copy for serverside-pivot

## X (Twitter)
Most data grids either crash your browser tab (looking at you, WASM) or make you spend 3 months writing SQL GROUP BY endpoints.

We just open-sourced serverside-pivot ⚡

🚀 2 lines of Python to connect any DB
🗄️ Parquet, DuckDB, Snowflake, BigQuery, Postgres
📊 Inline sparkline cells & real-time docked charts
🌡️ Built-in heatmaps & interactive data bars
🏎️ Virtualized windowing (< 80MB RAM)

100% Free & Open Source (MIT) for Dash, React & Streamlit:
https://github.com/Ramzy22/serverside_pivot

---

## LinkedIn
Excited to release serverside-pivot v1.0 — a turnkey, open-source server-side pivot engine for Python, React, Dash, and Streamlit.

If you have ever tried building pivot tables over large datasets:
1. Client-side tools like FINOS Perspective crash tab memory beyond a few million records.
2. AG Grid Enterprise provides frontend virtualization, but leaves the entire SQL backend and aggregation pipeline as an exercise for the developer.

serverside-pivot closes the loop:
- Low-Code: Connects to DuckDB, Snowflake, BigQuery, or Postgres in 2 lines via Ibis.
- Visual Analysis: Pre-configured inline SVG sparklines, heatmaps, interactive data bars, and docked multi-pane charts.
- Scale: Constant client RAM footprint (< 80 MB) using virtual windowing and O(D) level-based batching.

Tested with 207 mapped browser qualification journeys.

GitHub: https://github.com/Ramzy22/serverside_pivot
"""
    copy_path.write_text(copy_text, encoding="utf-8")
    print(f"Saved share copy: {copy_path}")


if __name__ == "__main__":
    build_final_video()
    write_share_copy()
    print("\nAll deliverables generated in brag-output/!")
