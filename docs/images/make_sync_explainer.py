"""Render docs/images/sync-explainer.gif, the animation at the top of the README.

Run from the repository root (needs Pillow and the DejaVu fonts):

    python docs/images/make_sync_explainer.py

The animation is drawn frame by frame, so it can be regenerated whenever the
app's behaviour changes. Scenes, in order:

1. A DocType's records in ERPNext; the warehouse is empty.
2. Records are copied in batches; the position is saved after each one.
3. Later, only edited records are copied again.
4. Reports query the warehouse.
5. A deleted document is removed from the copy after the delete commits.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

W, H = 880, 372
SCALE = 2  # draw at 2x, then downscale for smooth edges
FPS_MS = 70  # duration of one moving frame

OUT = Path(__file__).with_name("sync-explainer.gif")
FONT = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
FONT_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

INK = (17, 24, 39)
MUTED = (107, 114, 128)
LINE = (229, 231, 235)
BG = (255, 255, 255)
ERP = (37, 99, 235)
ERP_SOFT = (239, 246, 255)
WH = (180, 120, 0)
WH_SOFT = (255, 251, 235)
SYNC = (79, 70, 229)
EDIT = (234, 88, 12)
EDIT_SOFT = (255, 237, 213)
DEL = (220, 38, 38)
OK = (22, 163, 74)

RECORDS = [f"SINV-000{i}" for i in range(1, 7)]

# Panel geometry (in 1x pixels).
LEFT = (24, 40, 300, 316)
RIGHT = (580, 40, 856, 316)
ROW_H = 30
ROW_TOP = 110
LANE_Y = 150


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(FONT_BOLD if bold else FONT, size * SCALE)


def s(v: float) -> int:
    return int(round(v * SCALE))


def ease(t: float) -> float:
    t = max(0.0, min(1.0, t))
    return t * t * (3 - 2 * t)


def row_box(panel: tuple, index: int) -> tuple:
    x0, _, x1, _ = panel
    y = ROW_TOP + index * ROW_H
    return (x0 + 16, y, x1 - 16, y + ROW_H - 6)


class Frame:
    def __init__(self) -> None:
        self.img = Image.new("RGB", (s(W), s(H)), BG)
        self.d = ImageDraw.Draw(self.img)

    def rect(self, box, fill=None, outline=None, width=1, radius=8):
        x0, y0, x1, y1 = box
        self.d.rounded_rectangle(
            (s(x0), s(y0), s(x1), s(y1)), radius=s(radius), fill=fill, outline=outline, width=s(width)
        )

    def text(self, xy, txt, size=13, fill=INK, bold=False, anchor="la"):
        self.d.text((s(xy[0]), s(xy[1])), txt, font=font(size, bold), fill=fill, anchor=anchor)

    def line(self, pts, fill=LINE, width=2, dash=0):
        if not dash:
            self.d.line([(s(x), s(y)) for x, y in pts], fill=fill, width=s(width))
            return
        (x0, y0), (x1, y1) = pts
        length = x1 - x0
        x = x0
        while x < x1:
            self.d.line([(s(x), s(y0)), (s(min(x + dash, x1)), s(y1))], fill=fill, width=s(width))
            x += dash * 2
        _ = length

    def done(self) -> Image.Image:
        return self.img.resize((W, H), Image.LANCZOS)


def blend(a, b, t):
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(3))


def record_chip(f: Frame, box, label, *, accent=ERP, soft=ERP_SOFT, tag=None, tag_color=None,
                strike=0.0, fade=0.0):
    fill = blend(soft, BG, fade)
    outline = blend(accent, BG, fade)
    ink = blend(INK, BG, fade)
    f.rect(box, fill=fill, outline=outline, width=1, radius=6)
    x0, y0, x1, y1 = box
    f.text((x0 + 10, (y0 + y1) / 2), label, size=12, fill=ink, anchor="lm")
    if strike > 0:
        mid = (y0 + y1) / 2
        f.line([(x0 + 8, mid), (x0 + 8 + (x1 - x0 - 16) * strike, mid)], fill=DEL, width=2)
    if tag:
        f.text((x1 - 10, (y0 + y1) / 2), tag, size=11, fill=blend(tag_color or MUTED, BG, fade),
               bold=True, anchor="rm")


def scene_base(state: dict) -> Frame:
    """Panels, the sync lane and every record in its current state."""
    f = Frame()
    # Panels.
    f.rect(LEFT, fill=BG, outline=LINE, width=2, radius=12)
    f.rect(RIGHT, fill=BG, outline=LINE, width=2, radius=12)
    f.text((LEFT[0] + 16, LEFT[1] + 16), "ERPNext", size=15, bold=True, fill=ERP)
    f.text((LEFT[0] + 16, LEFT[1] + 40), "MariaDB · your live data", size=11, fill=MUTED)
    f.text((RIGHT[0] + 16, RIGHT[1] + 16), "ClickHouse", size=15, bold=True, fill=WH)
    f.text((RIGHT[0] + 16, RIGHT[1] + 40), "warehouse · reporting copy", size=11, fill=MUTED)

    # Sync lane in the middle; records in transit travel along it, under the badge.
    lane_y = LANE_Y
    f.line([(LEFT[2] + 8, lane_y), (RIGHT[0] - 8, lane_y)], fill=LINE, width=2, dash=6)
    for draw in state.get("flying", []):
        draw(f)
    badge = (370, lane_y - 14, 510, lane_y + 14)
    f.rect(badge, fill=SYNC, outline=None, radius=14)
    f.text(((badge[0] + badge[2]) / 2, (badge[1] + badge[3]) / 2), "ClickHouse Sync",
           size=12, fill=BG, bold=True, anchor="mm")
    if state.get("status"):
        f.text((440, lane_y - 52), state["status"], size=12, fill=SYNC, bold=True, anchor="mm")
    if state.get("substatus"):
        f.text((440, lane_y - 34), state["substatus"], size=11, fill=MUTED, anchor="mm")

    # Source rows.
    for i, name in enumerate(RECORDS):
        st = state["src"].get(name, {})
        if st.get("gone"):
            continue
        record_chip(
            f, row_box(LEFT, i), name,
            accent=st.get("accent", ERP), soft=st.get("soft", ERP_SOFT),
            tag=st.get("tag"), tag_color=st.get("tag_color"),
            strike=st.get("strike", 0.0), fade=st.get("fade", 0.0),
        )
    # Warehouse rows.
    for i, name in enumerate(RECORDS):
        st = state["wh"].get(name)
        if not st:
            continue
        record_chip(
            f, row_box(RIGHT, i), name, accent=WH, soft=WH_SOFT,
            tag=st.get("tag"), tag_color=st.get("tag_color", OK), fade=st.get("fade", 0.0),
        )

    # Report chart (scene 4 onwards).
    chart = state.get("chart", 0.0)
    if chart > 0:
        cx0, cy1 = 365, 300
        f.text((440, 204), "Reports & dashboards", size=11, fill=MUTED, anchor="mm")
        f.text((440, 220), "query ClickHouse", size=11, fill=MUTED, anchor="mm")
        heights = [26, 42, 34, 56, 46]
        for k, h in enumerate(heights):
            hh = h * ease(chart * 1.4 - k * 0.1)
            if hh <= 0:
                continue
            x = cx0 + k * 30
            f.rect((x, cy1 - hh, x + 20, cy1), fill=blend(WH, BG, 0.35), outline=None, radius=3)
        f.line([(cx0 - 6, cy1), (cx0 + 146, cy1)], fill=LINE, width=2)

    # Caption bar.
    f.rect((24, 330, 856, 362), fill=CAPTION_BG, outline=LINE, width=1, radius=8)
    f.text((40, 346), state["caption"], size=13, fill=INK, anchor="lm")
    return f


def flying_chips(names, t, *, accent=ERP, soft=ERP_SOFT):
    """Drawers for compact chips travelling along the lane, left to right."""
    def draw(f: Frame) -> None:
        # Stay inside the lane between the panels; the badge passes over them.
        start, end = LEFT[2] + 8, RIGHT[0] - 128
        for k, name in enumerate(names):
            local = ease((t - k * 0.15) / 0.7)
            if local <= 0 or local >= 1:
                continue
            x = start + (end - start) * local
            box = (x, LANE_Y - 11, x + 120, LANE_Y + 11)
            record_chip(f, box, name, accent=blend(accent, WH, local), soft=blend(soft, WH_SOFT, local))
    return [draw]


def build() -> tuple[list[Image.Image], list[int]]:
    frames: list[Image.Image] = []
    durations: list[int] = []

    def hold(state, ms):
        frames.append(scene_base(state).done())
        durations.append(ms)

    def animate(state_fn, n):
        for k in range(n):
            t = k / (n - 1)
            f = state_fn(t)
            frames.append(f.done())
            durations.append(FPS_MS)

    src = {name: {} for name in RECORDS}
    wh: dict[str, dict] = {}

    # 1. Starting point.
    cap1 = "1. Choose a DocType. ClickHouse Sync keeps a copy of its records in ClickHouse."
    hold({"src": src, "wh": wh, "caption": cap1}, 1800)

    # 2. Two batches.
    cap2 = "2. Records are copied in batches, and progress is saved after each batch."
    for batch_no, names in ((1, RECORDS[:3]), (2, RECORDS[3:])):
        def moving(t, names=names, batch_no=batch_no):
            st = {"src": src, "wh": wh, "caption": cap2,
                  "status": f"Batch {batch_no} of 2", "substatus": "copying…",
                  "flying": flying_chips(names, t)}
            return scene_base(st)
        animate(moving, 16)
        for n in names:
            wh[n] = {"tag": "✓", "tag_color": OK}
        hold({"src": src, "wh": wh, "caption": cap2,
              "status": f"Batch {batch_no} of 2", "substatus": "position saved"}, 700)

    # 3. Edits: only changed records are copied again.
    cap3 = "3. On the next run (every 15 minutes by default) only new and changed records are copied."
    edited = ["SINV-0002", "SINV-0005"]
    for n in edited:
        src[n] = {"accent": EDIT, "soft": EDIT_SOFT, "tag": "edited", "tag_color": EDIT}
    hold({"src": src, "wh": wh, "caption": cap3, "status": "Waiting for the next run"}, 1300)

    def resync(t):
        st = {"src": src, "wh": wh, "caption": cap3, "status": "2 changed records",
              "substatus": "copying…",
              "flying": flying_chips(edited, t, accent=EDIT, soft=EDIT_SOFT)}
        return scene_base(st)
    animate(resync, 16)
    for n in edited:
        src[n] = {}
        wh[n] = {"tag": "updated", "tag_color": OK}
    hold({"src": src, "wh": wh, "caption": cap3, "status": "Up to date"}, 1100)
    for n in edited:
        wh[n] = {"tag": "✓", "tag_color": OK}

    # 4. Reports read the warehouse.
    cap4 = "4. Reports and dashboards query ClickHouse, so ERPNext itself stays fast."

    def reports(t):
        return scene_base({"src": src, "wh": wh, "caption": cap4, "chart": t})
    animate(reports, 14)
    hold({"src": src, "wh": wh, "caption": cap4, "chart": 1.0}, 1300)

    # 5. A delete.
    cap5 = "5. Deleted documents are removed from the copy too, once the delete is saved."
    victim = "SINV-0003"

    def deleting(t):
        src[victim] = {"strike": ease(t), "accent": DEL, "tag": "deleted", "tag_color": DEL}
        return scene_base({"src": src, "wh": wh, "caption": cap5, "chart": 1.0,
                           "status": "Delete queued", "substatus": "applied within minutes"})
    animate(deleting, 10)

    def fading(t):
        src[victim] = {"strike": 1.0, "accent": DEL, "tag": "deleted", "tag_color": DEL, "fade": ease(t)}
        wh[victim] = {"tag": "removed", "tag_color": DEL, "fade": ease(t)}
        return scene_base({"src": src, "wh": wh, "caption": cap5, "chart": 1.0,
                           "status": "Delete applied", "substatus": "one statement per batch"})
    animate(fading, 14)
    src[victim] = {"gone": True}
    wh.pop(victim, None)

    cap6 = "ERPNext keeps working as usual; the ClickHouse copy stays a few minutes behind."
    hold({"src": src, "wh": wh, "caption": cap6, "chart": 1.0, "status": "Up to date"}, 2600)
    return frames, durations


CAPTION_BG = (249, 250, 251)


def _palette() -> Image.Image:
    colours = [BG, INK, MUTED, LINE, ERP, ERP_SOFT, WH, WH_SOFT, SYNC, EDIT, EDIT_SOFT, DEL, OK,
               CAPTION_BG, blend(WH, BG, 0.35)]
    backgrounds = [BG, ERP_SOFT, WH_SOFT, EDIT_SOFT, SYNC, CAPTION_BG]
    entries: list[tuple] = []
    for c in colours:
        for bg in backgrounds:
            for t in (0.0, 0.3, 0.55, 0.8):
                e = blend(c, bg, t)
                if e not in entries:
                    entries.append(e)
    entries = entries[:256]
    flat = [v for e in entries for v in e] + [0] * (768 - 3 * len(entries))
    pal = Image.new("P", (1, 1))
    pal.putpalette(flat)
    return pal


def main() -> None:
    frames, durations = build()
    # One fixed palette: every colour the drawing uses, plus its anti-aliasing
    # blends towards each background it sits on. Colours stay exact (a palette
    # learned from the frames merged the delete scene's red into orange) and
    # identical between frames, which keeps the file small.
    paletted = [fr.quantize(palette=_palette(), dither=Image.Dither.NONE) for fr in frames]
    paletted[0].save(
        OUT, save_all=True, append_images=paletted[1:], duration=durations, loop=0,
        optimize=True, disposal=1,
    )
    total = sum(durations) / 1000
    print(f"wrote {OUT} ({len(frames)} frames, {total:.1f} s, {OUT.stat().st_size / 1024:.0f} KiB)")


if __name__ == "__main__":
    main()
