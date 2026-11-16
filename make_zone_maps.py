"""
Zone maps: how weather and traffic affect delivery reliability across the 8 Chicago zones.
Reads data/silver/silver_orders.csv and writes
  diagrams/zone_weather_traffic_map.png / .pdf   (3-panel static map)
  diagrams/zone_weather_traffic_map.html         (interactive Leaflet map, needs internet for map tiles)
Run: python make_zone_maps.py
"""
import json
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt, numpy as np
from matplotlib.colors import Normalize, TwoSlopeNorm
from matplotlib.patches import Polygon
import pandas as pd, json
import os
H=os.path.dirname(os.path.abspath(__file__))
Z={"Z01":("Hyde Park",41.7943,-87.5907),"Z02":("Loop",41.8837,-87.6289),"Z03":("South Loop",41.8580,-87.6270),
   "Z04":("West Loop",41.8827,-87.6533),"Z05":("Lincoln Park",41.9214,-87.6513),"Z06":("Wicker Park",41.9088,-87.6796),
   "Z07":("Logan Square",41.9234,-87.7083),"Z08":("Pilsen",41.8525,-87.6631)}
def stats():
    o=pd.read_csv(f"{H}/data/silver/silver_orders.csv"); d=o[o.order_status=="delivered"].copy()
    d["wet"]=d.weather_severity>=1; d["cong"]=d.congestion_level!="free"
    rate=lambda x: None if len(x)==0 else round(100*x.is_late.mean(),1)
    rows=[]
    for z,(name,lat,lon) in Z.items():
        x=d[d.zone_id==z]
        rows.append(dict(zone_id=z,zone=name,lat=lat,lon=lon,orders=int((o.zone_id==z).sum()),delivered=len(x),
            late=rate(x),late_dry=rate(x[~x.wet]),late_wet=rate(x[x.wet]),n_wet=int(x.wet.sum()),
            late_free=rate(x[~x.cong]),late_cong=rate(x[x.cong]),n_cong=int(x.cong.sum()),
            pct_cong=round(100*x.cong.mean(),1),avg_min=round(x.delivery_min.mean(),1),
            avg_wx_delay=round(x.sim_weather_delay_min.mean(),2),avg_tr_delay=round(x.sim_traffic_delay_min.mean(),2),
            speed=round(x.typical_speed_mph.mean(),1)))
    tot=dict(late=rate(d),late_dry=rate(d[~d.wet]),late_wet=rate(d[d.wet]),late_free=rate(d[~d.cong]),late_cong=rate(d[d.cong]))
    return pd.DataFrame(rows), tot


def static_map(df,t):
    
    # approximate Lake Michigan shoreline (lat, lon), north -> south
    SHORE=[(41.97,-87.650),(41.95,-87.638),(41.93,-87.630),(41.91,-87.624),(41.895,-87.614),(41.885,-87.612),(41.87,-87.612),
           (41.855,-87.609),(41.84,-87.605),(41.825,-87.598),(41.81,-87.590),(41.795,-87.578),(41.78,-87.572),(41.765,-87.562)]
    XL,YL=(-87.735,-87.545),(41.765,41.965)
    lake=[(lo,la) for la,lo in SHORE]+[(XL[1]+.05,YL[0]-.05),(XL[1]+.05,YL[1]+.05)]
    fig,axes=plt.subplots(1,3,figsize=(17,7.6),dpi=200)
    panels=[("Overall late-delivery rate","late",Normalize(18,42),"YlOrRd","orders",
             lambda r:f"{r.late:.1f}%", "% of delivered orders late"),
            ("Rain effect: late rate on wet vs. dry hours","rain",TwoSlopeNorm(0,-10,30),"RdYlGn_r","n_wet",
             lambda r:(f"{r.late_dry:.0f}% → {r.late_wet:.0f}%\n({r.late_wet-r.late_dry:+.1f} pts)" if r.n_wet>=30 else f"only {r.n_wet} wet-hour orders\n(too few to compare)"), "change in late rate, wet minus dry (points)"),
            ("Traffic exposure: orders placed in congested traffic","pct_cong",Normalize(0,100),"Blues","delivered",
             lambda r:(f"{r.pct_cong:.0f}% congested\nlate {r.late_free:.0f}% → {r.late_cong:.0f}%" if r.n_cong>=30 and r.delivered-r.n_cong>=30 else f"{r.pct_cong:.0f}% congested"),
             "% of orders in moderate or heavy traffic")]
    df["rain"]=(df.late_wet-df.late_dry).where(df.n_wet>=30)
    LAB={"Logan Square":(-87.733,41.952),"Wicker Park":(-87.733,41.896),"Lincoln Park":(-87.632,41.944),"West Loop":(-87.733,41.873),
         "Loop":(-87.607,41.884),"South Loop":(-87.604,41.852),"Pilsen":(-87.733,41.836),"Hyde Park":(-87.690,41.792)}
    for ax,(title,col,norm,cmap,size,lab,cbl) in zip(axes,panels):
        ax.set_facecolor("#F3F1EC"); ax.add_patch(Polygon(lake,fc="#CFE3F2",ec="#8FB4D3",lw=1.2,zorder=1))
        ax.text(-87.565,41.90,"Lake\nMichigan",color="#5A86AD",fontsize=10,style="italic",ha="center")
        s=df[size].to_numpy(); na=df[col].isna()
        ax.scatter(df.lon[na],df.lat[na],s=(150+1100*s/s.max())[na],c="#C8C8C8",ec="#333",lw=.8,zorder=3)
        sc=ax.scatter(df.lon,df.lat,s=150+1100*s/s.max(),c=df[col],cmap=cmap,norm=norm,ec="#333",lw=.8,zorder=3,alpha=.92)
        for r in df.itertuples():
            x,y=LAB[r.zone]; ax.annotate(f"{r.zone}\n{lab(r)}",(r.lon,r.lat),(x,y),fontsize=7.6,zorder=5,ha="left",va="center",
                bbox=dict(fc="white",ec="#BBB",lw=.5,alpha=.95,pad=1.5),
                arrowprops=dict(arrowstyle="-",color="#777",lw=.7,shrinkA=0,shrinkB=4))
        ax.set_xlim(*XL); ax.set_ylim(*YL); ax.set_aspect(1/np.cos(np.radians(41.87)))
        ax.set_xticks([]); ax.set_yticks([]); ax.set_title(title,fontsize=11.5,fontweight="bold",color="#1F3864")
        cb=fig.colorbar(sc,ax=ax,orientation="horizontal",fraction=.045,pad=.03); cb.set_label(cbl,fontsize=8.5); cb.ax.tick_params(labelsize=8)
    fig.suptitle("Where weather and traffic hurt delivery reliability (8 Chicago delivery zones, Aug 1 to Oct 24, 2026)",fontsize=13.5,fontweight="bold",color="#1F3864",y=.98)
    fig.text(.5,.015,f"All zones: late rate {t['late_dry']:.1f}% on dry hours vs {t['late_wet']:.1f}% on wet hours, and {t['late_free']:.1f}% in free traffic vs {t['late_cong']:.1f}% in congested traffic.  "
             "Bubble size = order volume.  Weather from Open-Meteo, typical traffic from the Chicago Traffic Tracker (2025 profile).  Delays are simulated on synthetic orders.  Shoreline is approximate.  Grey = fewer than 30 orders.",
             ha="center",fontsize=8,color="#444",wrap=True)
    plt.tight_layout(rect=(0,.04,1,.95))
    for ext in ("png","pdf"): fig.savefig(f"{H}/diagrams/zone_weather_traffic_map.{ext}")

def html_map(df,t):
    
    data=json.loads(df.to_json(orient="records"))
    html=r"""<!DOCTYPE html>
    <html lang="en"><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Weather and traffic vs. delivery reliability: Chicago zones</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
    <style>
    :root{color-scheme:light}
    body{margin:0;font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;color:#1d2433;background:#fafafa}
    header{padding:14px 20px 8px}
    h1{font-size:19px;margin:0 0 4px;color:#1F3864}
    p.sub{margin:0;font-size:13px;color:#555}
    .wrap{display:flex;gap:14px;padding:8px 20px 20px;flex-wrap:wrap}
    #map{flex:1 1 620px;height:620px;border-radius:8px;border:1px solid #ddd}
    aside{flex:0 1 330px;font-size:13px}
    .btns{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px}
    .btns button{border:1px solid #c9cfdb;background:#fff;border-radius:16px;padding:6px 11px;font-size:12.5px;cursor:pointer}
    .btns button.on{background:#1F3864;color:#fff;border-color:#1F3864}
    .card{background:#fff;border:1px solid #e1e4ea;border-radius:8px;padding:10px 12px;margin-bottom:10px}
    .card h3{margin:0 0 6px;font-size:13.5px;color:#1F3864}
    table{border-collapse:collapse;width:100%;font-size:12.5px}
    td,th{padding:3px 4px;text-align:right;border-bottom:1px solid #eee}
    td:first-child,th:first-child{text-align:left}
    .legend{background:#fff;padding:8px 10px;border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,.2);font-size:12px;line-height:1.5}
    .bar{height:10px;width:170px;border-radius:3px;margin:4px 0}
    .note{font-size:11.5px;color:#666}
    </style></head><body>
    <header><h1>Where weather and traffic hurt delivery reliability</h1>
    <p class="sub">8 Chicago delivery zones · Aug 1 to Oct 24, 2026 · circle size = order volume · click a zone for details</p></header>
    <div class="wrap"><div id="map"></div>
    <aside>
    <div class="btns" id="btns"></div>
    <div class="card" id="explain"></div>
    <div class="card"><h3>All zones</h3><table>
    <tr><td>Late rate, all delivered orders</td><td><b>__LATE__%</b></td></tr>
    <tr><td>Dry hours → wet hours</td><td>__DRY__% → <b>__WET__%</b></td></tr>
    <tr><td>Free traffic → congested</td><td>__FREE__% → <b>__CONG__%</b></td></tr></table></div>
    <div class="card note">Weather: Open-Meteo hourly history per zone. Traffic: typical speed per zone, weekday and hour from the City of Chicago Traffic Tracker (Aug–Oct 2025 profile). Orders are synthetic, with documented rule-based weather and traffic delays (see the Stage 3 report). Rates use delivered orders only. Cells with fewer than 30 orders are shown as "n/a".</div>
    </aside></div>
    <script>
    const Z=__DATA__;
    const M={
     late:{label:"Overall late rate",get:z=>z.late,min:18,max:42,pal:"heat",unit:"%",txt:"Share of delivered orders that arrived more than 10 minutes after the quoted ETA."},
     late_dry:{label:"Late rate, dry hours",get:z=>z.late_dry,min:18,max:52,pal:"heat",unit:"%",txt:"Late rate for orders placed in hours with no rain, drizzle or snow."},
     late_wet:{label:"Late rate, wet hours",get:z=>z.n_wet>=30?z.late_wet:null,min:18,max:52,pal:"heat",unit:"%",txt:"Late rate for orders placed while it was raining, drizzling or snowing in that zone."},
     rain:{label:"Rain effect (pts)",get:z=>z.n_wet>=30?z.late_wet-z.late_dry:null,min:-10,max:30,pal:"div",unit:" pts",txt:"How many percentage points the late rate rises on wet hours compared with dry hours."},
     pct_cong:{label:"Congested share",get:z=>z.pct_cong,min:0,max:100,pal:"blue",unit:"%",txt:"Share of orders placed when typical traffic in the zone was moderate or heavy (under 22 mph)."},
     late_cong:{label:"Late rate, congested",get:z=>(z.n_cong>=30)?z.late_cong:null,min:18,max:52,pal:"heat",unit:"%",txt:"Late rate for orders placed in moderate or heavy traffic."}
    };
    const pals={heat:["#fff5b8","#fec44f","#fb6a4a","#cb181d","#67000d"],blue:["#f7fbff","#9ecae1","#4292c6","#08519c","#08306b"],div:["#1a9850","#d9ef8b","#ffffbf","#fc8d59","#a50026"]};
    function lerp(a,b,t){const p=x=>parseInt(x,16);return "#"+[1,3,5].map(i=>Math.round(p(a.substr(i,2))+(p(b.substr(i,2))-p(a.substr(i,2)))*t).toString(16).padStart(2,"0")).join("")}
    function color(m,v){if(v==null)return "#bdbdbd";const c=pals[m.pal];let t=(v-m.min)/(m.max-m.min);if(m.pal==="div"){t=v<0?0.5*(v-m.min)/(0-m.min):0.5+0.5*v/m.max}
     t=Math.max(0,Math.min(1,t))*(c.length-1);const i=Math.min(Math.floor(t),c.length-2);return lerp(c[i],c[i+1],t-i)}
    const fmt=(v,u)=>v==null?"n/a":(u===" pts"&&v>0?"+":"")+v.toFixed(1)+u;
    const map=L.map("map",{scrollWheelZoom:false}).setView([41.873,-87.645],12);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{maxZoom:17,attribution:"&copy; OpenStreetMap contributors"}).addTo(map);
    const maxO=Math.max(...Z.map(z=>z.orders));
    const circles=Z.map(z=>{const c=L.circleMarker([z.lat,z.lon],{radius:12+26*Math.sqrt(z.orders/maxO),weight:1.5,color:"#333",fillOpacity:.82}).addTo(map);
     c.bindPopup(`<b>${z.zone}</b> (${z.orders} orders)<table>
     <tr><td>Late rate</td><td>${fmt(z.late,"%")}</td></tr>
     <tr><td>Dry → wet hours</td><td>${fmt(z.late_dry,"%")} → ${z.n_wet>=30?fmt(z.late_wet,"%"):"n/a"}</td></tr>
     <tr><td>Free → congested traffic</td><td>${(z.delivered-z.n_cong)>=30?fmt(z.late_free,"%"):"n/a"} → ${z.n_cong>=30?fmt(z.late_cong,"%"):"n/a"}</td></tr>
     <tr><td>Orders in congestion</td><td>${z.pct_cong.toFixed(0)}%</td></tr>
     <tr><td>Typical speed</td><td>${z.speed} mph</td></tr>
     <tr><td>Avg delivery</td><td>${z.avg_min} min</td></tr>
     <tr><td>Avg added delay: weather / traffic</td><td>${z.avg_wx_delay} / ${z.avg_tr_delay} min</td></tr></table>`);
     c.bindTooltip(z.zone,{permanent:true,direction:"top",offset:[0,-10],className:""});return c});
    const legend=L.control({position:"bottomleft"});legend.onAdd=()=>{const d=L.DomUtil.create("div","legend");d.id="legend";return d};legend.addTo(map);
    function show(k){const m=M[k];
     document.querySelectorAll("#btns button").forEach(b=>b.classList.toggle("on",b.dataset.k===k));
     circles.forEach((c,i)=>{const v=m.get(Z[i]);c.setStyle({fillColor:color(m,v)});c.setTooltipContent(`${Z[i].zone}: ${fmt(v,m.unit)}`)});
     const c=pals[m.pal];document.getElementById("legend").innerHTML=`<b>${m.label}</b><div class="bar" style="background:linear-gradient(90deg,${c.join(",")})"></div>
      <div style="display:flex;justify-content:space-between;width:170px"><span>${m.min}${m.unit}</span><span>${m.max}${m.unit}</span></div><span style="color:#888">grey = n/a (under 30 orders)</span>`;
     const rows=Z.map(z=>[z.zone,m.get(z)]).sort((a,b)=>(b[1]??-999)-(a[1]??-999));
     document.getElementById("explain").innerHTML=`<h3>${m.label}</h3><div class="note" style="margin-bottom:6px">${m.txt}</div><table>${rows.map(r=>`<tr><td>${r[0]}</td><td>${fmt(r[1],m.unit)}</td></tr>`).join("")}</table>`}
    const bt=document.getElementById("btns");Object.entries(M).forEach(([k,m])=>{const b=document.createElement("button");b.textContent=m.label;b.dataset.k=k;b.onclick=()=>show(k);bt.appendChild(b)});
    show("late");
    </script></body></html>"""
    html=(html.replace("__DATA__",json.dumps(data)).replace("__LATE__",f"{t['late']:.1f}").replace("__DRY__",f"{t['late_dry']:.1f}")
          .replace("__WET__",f"{t['late_wet']:.1f}").replace("__FREE__",f"{t['late_free']:.1f}").replace("__CONG__",f"{t['late_cong']:.1f}"))
    open(f"{H}/diagrams/zone_weather_traffic_map.html","w").write(html); print("ok")

if __name__ == "__main__":
    df, t = stats()
    static_map(df, t)
    html_map(df, t)
    print("wrote diagrams/zone_weather_traffic_map.{png,pdf,html}")
