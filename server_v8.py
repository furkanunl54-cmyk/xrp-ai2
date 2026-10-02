from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from urllib.request import Request, urlopen
from urllib.parse import urlparse, parse_qs
import json, time, math, statistics, os, threading
from concurrent.futures import ThreadPoolExecutor

HOST=os.environ.get('HOST','0.0.0.0'); PORT=int(os.environ.get('PORT','8769'))
BASE=os.path.dirname(os.path.abspath(__file__))
CACHE={}

def get_json(url, method='GET', body=None, headers=None, timeout=8):
    h={'User-Agent':'XRP-AI-Terminal/1.0','Accept':'application/json'}
    if headers: h.update(headers)
    data=None
    if body is not None:
        data=json.dumps(body).encode(); h['Content-Type']='application/json'
    req=Request(url,data=data,headers=h,method=method)
    with urlopen(req,timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))

def safe(name, fn):
    try: return fn()
    except Exception as e: return {'_error':str(e),'_source':name}

def ema(vals,p):
    if len(vals)<p: return vals[-1] if vals else 0
    k=2/(p+1); e=vals[0]
    for x in vals[1:]: e=x*k+e*(1-k)
    return e

def rsi(vals,p=14):
    if len(vals)<p+2:return 50
    gains=[]; losses=[]
    for i in range(1,len(vals)):
        d=vals[i]-vals[i-1]; gains.append(max(d,0)); losses.append(max(-d,0))
    ag=sum(gains[:p])/p; al=sum(losses[:p])/p
    for i in range(p,len(gains)):
        ag=(ag*(p-1)+gains[i])/p; al=(al*(p-1)+losses[i])/p
    return 100 if al==0 else 100-100/(1+ag/al)

def boll(vals,p=20):
    a=vals[-p:]; m=sum(a)/len(a); s=statistics.pstdev(a) if len(a)>1 else 0
    return m,m+2*s,m-2*s

def atr_series(highs, lows, closes, p=14):
    out=[]
    for i in range(len(closes)):
        if i==0: tr=highs[i]-lows[i]
        else: tr=max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        out.append(tr)
    vals=[]
    for i in range(len(out)):
        vals.append(sum(out[max(0,i-p+1):i+1])/min(p,i+1))
    return vals

def adx_last(highs,lows,closes,p=14):
    if len(closes)<p*2+1: return 20
    trs=[]; plus=[]; minus=[]
    for i in range(1,len(closes)):
        up=highs[i]-highs[i-1]; dn=lows[i-1]-lows[i]
        trs.append(max(highs[i]-lows[i],abs(highs[i]-closes[i-1]),abs(lows[i]-closes[i-1])))
        plus.append(up if up>dn and up>0 else 0); minus.append(dn if dn>up and dn>0 else 0)
    tr=sum(trs[:p]); pp=sum(plus[:p]); mm=sum(minus[:p]); dx=[]
    for i in range(p,len(trs)):
        tr=tr-tr/p+trs[i]; pp=pp-pp/p+plus[i]; mm=mm-mm/p+minus[i]
        pdi=100*pp/tr if tr else 0; mdi=100*mm/tr if tr else 0
        dx.append(100*abs(pdi-mdi)/(pdi+mdi) if pdi+mdi else 0)
    return sum(dx[-p:])/min(p,len(dx)) if dx else 20

def vwap_last(highs,lows,closes,volumes,p=50):
    lo=max(0,len(closes)-p); pv=0; vv=0
    for i in range(lo,len(closes)):
        tp=(highs[i]+lows[i]+closes[i])/3; pv+=tp*volumes[i]; vv+=volumes[i]
    return pv/vv if vv else closes[-1]

def clamp(x): return max(0,min(100,x))

def spot_ticker(symbol): return get_json(f'https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}')

def klines(symbol, interval='1h', limit=500): return get_json(f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}')

def build_snapshot():
    out={"ts":time.time(),"sources":{},"errors":[]}
    jobs={
      'xrp':lambda:spot_ticker('XRPUSDT'),'btc':lambda:spot_ticker('BTCUSDT'),
      'xk':lambda:klines('XRPUSDT'),'x15':lambda:klines('XRPUSDT','15m',500),'bk':lambda:klines('BTCUSDT'),
      'fund':lambda:get_json('https://fapi.binance.com/fapi/v1/premiumIndex?symbol=XRPUSDT'),
      'oi':lambda:get_json('https://fapi.binance.com/fapi/v1/openInterest?symbol=XRPUSDT'),
      'depth':lambda:get_json('https://fapi.binance.com/fapi/v1/depth?symbol=XRPUSDT&limit=100'),
      'oih':lambda:get_json('https://fapi.binance.com/futures/data/openInterestHist?symbol=XRPUSDT&period=1h&limit=2'),
      'ls':lambda:get_json('https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=XRPUSDT&period=1h&limit=1'),
      'taker':lambda:get_json('https://fapi.binance.com/futures/data/takerBuySellVol?symbol=XRPUSDT&contractType=PERPETUAL&period=1h&limit=1'),
      'liq':lambda:get_json('https://fapi.binance.com/fapi/v1/allForceOrders?symbol=XRPUSDT&limit=100'),
      # Public derivatives data; no API keys required.
      'okx_ticker':lambda:get_json('https://www.okx.com/api/v5/market/ticker?instId=XRP-USDT-SWAP'),
      'okx_funding':lambda:get_json('https://www.okx.com/api/v5/public/funding-rate?instId=XRP-USDT-SWAP'),
      'okx_oi':lambda:get_json('https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=XRP-USDT-SWAP'),
      'okx_book':lambda:get_json('https://www.okx.com/api/v5/market/books?instId=XRP-USDT-SWAP&sz=20'),
      'okx_trades':lambda:get_json('https://www.okx.com/api/v5/market/trades?instId=XRP-USDT-SWAP&limit=100'),
      'by_ticker':lambda:get_json('https://api.bybit.com/v5/market/tickers?category=linear&symbol=XRPUSDT'),
      'by_oi':lambda:get_json('https://api.bybit.com/v5/market/open-interest?category=linear&symbol=XRPUSDT&intervalTime=1h&limit=2'),
      'by_funding':lambda:get_json('https://api.bybit.com/v5/market/funding/history?category=linear&symbol=XRPUSDT&limit=2'),
      'by_book':lambda:get_json('https://api.bybit.com/v5/market/orderbook?category=linear&symbol=XRPUSDT&limit=50'),
      'by_ratio':lambda:get_json('https://api.bybit.com/v5/market/account-ratio?category=linear&symbol=XRPUSDT&period=1h&limit=1')
    }
    with ThreadPoolExecutor(max_workers=min(20,len(jobs))) as ex:
        fut={k:ex.submit(safe,k,v) for k,v in jobs.items()}
        res={k:f.result() for k,f in fut.items()}
    for k,v in res.items():
        if isinstance(v,dict) and '_error' in v: out['errors'].append(k); out['sources'][k]=False
        else: out['sources'][k]=True
    x=res['xrp']; b=res['btc']; xk=res['xk']; x15=res['x15']; bk=res['bk']
    if any(isinstance(z,dict) and '_error' in z for z in [x,b,xk,bk]): raise RuntimeError('Temel Binance spot verisi alınamadı')
    xv=[float(z[4]) for z in xk]; xh=[float(z[2]) for z in xk]; xl=[float(z[3]) for z in xk]; vols=[float(z[5]) for z in xk]; x15v=[float(z[4]) for z in x15] if isinstance(x15,list) else xv[-100:]; x15h=[float(z[2]) for z in x15] if isinstance(x15,list) else x15v; x15l=[float(z[3]) for z in x15] if isinstance(x15,list) else x15v; x15c=x15v; bv=[float(z[4]) for z in bk]
    price=float(x['lastPrice']); bprice=float(b['lastPrice']); btc24=float(b['priceChangePercent'])
    e20=ema(xv,20); e50=ema(xv,50); e200=ema(xv,200); rr=rsi(xv); m=ema(xv,12)-ema(xv,26); bm,bup,blow=boll(xv); atr1h=atr_series(xh,xl,xv,14)[-1]; adx1h=adx_last(xh,xl,xv,14); vwap1h=vwap_last(xh,xl,xv,vols,50)
    be20=ema(bv,20); be50=ema(bv,50); brr=rsi(bv)
    avgvol=sum(vols[-21:-1])/20 if len(vols)>21 else statistics.mean(vols)
    volratio=vols[-1]/avgvol if avgvol else 1
    tech=50+(12 if price>e20 else -12)+(15 if e20>e50 else -15)+(8 if 52<=rr<=68 else -10 if rr>75 else -7 if rr<35 else 0)+(8 if m>0 else -8)+(3 if bm<price<bup else -6 if price>bup else -3)
    if volratio>1.25: tech += 6 if price>e20 else -6
    tech += 5 if price>e200 else -5
    tech += 4 if price>vwap1h else -3
    tech += 4 if adx1h>=25 and e20>e50 else -2 if adx1h<18 else 0
    tech=clamp(tech)
    # Short-term 15m momentum for the user's hit-and-run spot style
    e15=ema(x15c,20); e15f=ema(x15c,50); r15=rsi(x15c); m15=ema(x15c,12)-ema(x15c,26)
    tr=[]
    for i in range(1,len(x15c)):
        tr.append(max(x15h[i]-x15l[i], abs(x15h[i]-x15c[i-1]), abs(x15l[i]-x15c[i-1])))
    atr15=sum(tr[-14:])/14 if len(tr)>=14 else max(price*0.005, 1e-9)
    short=50+(14 if price>e15 else -14)+(10 if e15>e15f else -10)+(10 if 48<=r15<=70 else -12 if r15>78 else -8 if r15<30 else 0)+(8 if m15>0 else -8)
    short=clamp(short)
    market=50+(10 if bprice>be20 else -10)+(15 if be20>be50 else -15)+(7 if 45<=brr<=70 else -8 if brr<35 else -5 if brr>78 else 0)+(10 if (price>e20)==(bprice>be20) else -6)+(-5 if abs(btc24)>5 else 0)
    market=clamp(market)
    fund=res['fund']; oi=res['oi']; oih=res['oih']; ls=res['ls']; taker=res['taker']
    futures=50; funding=None; oi_val=None; oi_ch=None; ls_val=None; taker_ratio=None
    if not isinstance(fund,dict) or '_error' not in fund:
        funding=float(fund.get('lastFundingRate',0)); futures += 8 if abs(funding)<.0005 else -14 if funding>.001 else 8 if funding<-.001 else 2
    if not isinstance(oi,dict) or '_error' not in oi: oi_val=float(oi.get('openInterest',0))
    if isinstance(oih,list) and len(oih)>=2:
        a=float(oih[0].get('sumOpenInterestValue',0)); z=float(oih[1].get('sumOpenInterestValue',0)); oi_ch=z/a-1 if a else None
        futures += 7 if oi_ch>.03 and price>e20 else -10 if oi_ch>.03 else -2 if oi_ch<-.03 else 2
    if isinstance(ls,list) and ls:
        ls_val=float(ls[0].get('longShortRatio',1)); futures += -12 if ls_val>2 else 7 if ls_val<.65 else 3
    if isinstance(taker,list) and taker:
        buy=float(taker[0].get('takerBuyVolValue',0)); sell=float(taker[0].get('takerSellVolValue',0)); taker_ratio=buy/sell if sell else None
        futures += 8 if taker_ratio and taker_ratio>1.15 else -8 if taker_ratio and taker_ratio<.87 else 2
    futures=clamp(futures)

    # Cross-exchange derivatives consensus: Binance + OKX + Bybit.
    # OI is compared by direction/change, not summed, because exchanges use different conventions.
    derivatives={}; deriv_scores=[]
    def book_stats(book):
        if not isinstance(book,dict): return None,None,None
        bids=book.get('bids') or book.get('result',{}).get('b',[])
        asks=book.get('asks') or book.get('result',{}).get('a',[])
        if not bids or not asks: return None,None,None
        def pq(x): return float(x[0]),float(x[1])
        bv=sum(pq(x)[0]*pq(x)[1] for x in bids); av=sum(pq(x)[0]*pq(x)[1] for x in asks)
        ratio=bv/av if av else None
        bid=pq(bids[0])[0]; ask=pq(asks[0])[0]
        return ratio,bid,ask
    # OKX
    try:
        ot=res['okx_ticker']; of=res['okx_funding']; oo=res['okx_oi']; ob=res['okx_book'];
        t=ot['data'][0]; funding_ok=float(of['data'][0].get('fundingRate',0)); oi_ok=float(oo['data'][0].get('oi',0)); price_ok=float(t['last'])
        ratio_ok,bid_ok,ask_ok=book_stats(ob['data'][0]); score=50
        score += 8 if abs(funding_ok)<.0005 else -12 if funding_ok>.001 else 5 if funding_ok<-.001 else 1
        score += 10 if ratio_ok and ratio_ok>1.12 else -10 if ratio_ok and ratio_ok<.89 else 2
        derivatives['OKX']={'score':clamp(score),'price':price_ok,'funding':funding_ok,'openInterest':oi_ok,'bookRatio':ratio_ok,'spread':((ask_ok-bid_ok)/price_ok if bid_ok and ask_ok else None),'available':True,'instrument':'XRP-USDT-SWAP'}
        deriv_scores.append(score)
    except Exception as e:
        derivatives['OKX']={'score':None,'available':False,'error':str(e),'instrument':'XRP-USDT-SWAP'}
    # Bybit
    try:
        bt=res['by_ticker']['result']['list'][0]; bf=res['by_funding']['result']['list']; bo=res['by_oi']['result']['list']; bb=res['by_book'];
        funding_by=float(bt.get('fundingRate', bf[0].get('fundingRate',0) if bf else 0)); oi_by=float(bt.get('openInterest',0)); price_by=float(bt['lastPrice'])
        oi_change_by=None
        if len(bo)>=2:
            a=float(bo[-1].get('openInterest',0)); z=float(bo[0].get('openInterest',0)); oi_change_by=z/a-1 if a else None
        ratio_by,bid_by,ask_by=book_stats(bb); score=50
        score += 8 if abs(funding_by)<.0005 else -12 if funding_by>.001 else 5 if funding_by<-.001 else 1
        score += 10 if ratio_by and ratio_by>1.12 else -10 if ratio_by and ratio_by<.89 else 2
        if oi_change_by is not None: score += 6 if oi_change_by>.03 and price_by>price else -8 if oi_change_by>.03 else 2
        derivatives['Bybit']={'score':clamp(score),'price':price_by,'funding':funding_by,'openInterest':oi_by,'oiChange':oi_change_by,'bookRatio':ratio_by,'spread':((ask_by-bid_by)/price_by if bid_by and ask_by else None),'available':True,'instrument':'XRPUSDT linear'}
        deriv_scores.append(score)
    except Exception as e:
        derivatives['Bybit']={'score':None,'available':False,'error':str(e),'instrument':'XRPUSDT linear'}
    derivatives_consensus=round(sum(deriv_scores)/len(deriv_scores)) if deriv_scores else None
    deriv_health=round(100*len(deriv_scores)/2)
    if derivatives_consensus is not None:
        futures=clamp(.70*futures+.30*derivatives_consensus)
    depth=res['depth']; micro=50; book_ratio=None; spread=None; mark_gap=None
    if isinstance(depth,dict) and depth.get('bids') and depth.get('asks'):
        bid=sum(float(p)*float(q) for p,q in depth['bids']); ask=sum(float(p)*float(q) for p,q in depth['asks']); book_ratio=bid/ask if ask else 1
        micro += 12 if book_ratio>1.15 else -12 if book_ratio<.87 else 2
        bp=float(depth['bids'][0][0]); ap=float(depth['asks'][0][0]); spread=(ap-bp)/price
    if isinstance(fund,dict) and 'markPrice' in fund: mark_gap=(float(fund['markPrice'])-price)/price
    if spread is not None: micro += 10 if spread<.0005 else -12 if spread>.002 else 2
    if mark_gap is not None: micro += 5 if abs(mark_gap)<.001 else -5
    micro=clamp(micro)
    # Multi-exchange spot consensus
    def cb(): return float(get_json('https://api.exchange.coinbase.com/products/XRP-USD/ticker')['price'])
    def kr():
        q=get_json('https://api.kraken.com/0/public/Ticker?pair=XRPUSD'); k=list(q['result'].keys())[0]; return float(q['result'][k]['c'][0])
    def okx(): return float(get_json('https://www.okx.com/api/v5/market/ticker?instId=XRP-USDT')['data'][0]['last'])
    def bybit(): return float(get_json('https://api.bybit.com/v5/market/tickers?category=spot&symbol=XRPUSDT')['result']['list'][0]['lastPrice'])
    ex=[price]
    for name,fn in [('Coinbase',cb),('Kraken',kr),('OKX',okx),('Bybit',bybit)]:
        try: ex.append(fn()); out['sources'][name]=True
        except Exception as e: out['sources'][name]=False; out['errors'].append(name)
    avg=sum(ex)/len(ex); exspread=(max(ex)-min(ex))/avg if avg else 0; spot_exscore=85 if exspread<.002 else 60 if exspread<.005 else 30; exscore=round(.65*spot_exscore+.35*deriv_health)
    # News via Google News RSS, parsed with stdlib XML
    import xml.etree.ElementTree as ET
    news_score=50; news_count=0; headlines=[]
    try:
        rss=get_json('https://api.rss2json.com/v1/api.json?rss_url=https%3A%2F%2Fnews.google.com%2Frss%2Fsearch%3Fq%3DXRP%2520Ripple%26hl%3Den-US%26gl%3DUS%26ceid%3DUS%253Aen')
        items=rss.get('items',[])[:15]; news_count=len(items); headlines=[i.get('title','') for i in items]
        pos=['etf','approval','approved','adoption','inflow','partnership','launch','surge','breakout','accumulate','bullish','legal victory']
        neg=['lawsuit','ban','hack','exploit','outflow','liquidation','delist','fraud','bearish','crash','selloff','rejection']
        p=sum(sum(w in h.lower() for w in pos) for h in headlines); n=sum(sum(w in h.lower() for w in neg) for h in headlines); news_score=clamp(50+(p-n)*5)
        out['sources']['News']=True
    except Exception as e: out['sources']['News']=False; out['errors'].append('News')
    # XRPL ledger activity + large transfer proxy. Not a certified whale detector.
    onchain=50; tx_count=None; large_count=None
    try:
        xr=get_json('https://xrplcluster.com/',method='POST',body={'method':'ledger','params':[{'ledger_index':'validated','transactions':True,'expand':True}]},headers={'Content-Type':'application/json'})
        tx=xr.get('result',{}).get('ledger',{}).get('transactions',[]); tx_count=len(tx)
        amounts=[]
        for t in tx:
            a=t.get('Amount')
            if isinstance(a,str): amounts.append(float(a)/1e6)
            elif isinstance(a,dict) and a.get('currency')=='XRP':
                try: amounts.append(float(a.get('value',0)))
                except: pass
        large_count=sum(v>=1_000_000 for v in amounts)
        onchain=70 if tx_count>300 else 60 if tx_count>150 else 40 if tx_count<50 else 50
        out['sources']['XRPL']=True
    except Exception as e: out['sources']['XRPL']=False; out['errors'].append('XRPL')
    # Macro via Yahoo chart endpoints
    macro=50; dxy_ch=None; tnx_ch=None
    try:
        q=get_json('https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?range=5d&interval=1d'); a=[x for x in q['chart']['result'][0]['indicators']['quote'][0]['close'] if x is not None]; dxy_ch=a[-1]/a[-2]-1; macro += 12 if dxy_ch<-.002 else -12 if dxy_ch>.002 else 0; out['sources']['DXY']=True
    except Exception as e: out['sources']['DXY']=False; out['errors'].append('DXY')
    try:
        q=get_json('https://query1.finance.yahoo.com/v8/finance/chart/%5ETNX?range=5d&interval=1d'); a=[x for x in q['chart']['result'][0]['indicators']['quote'][0]['close'] if x is not None]; tnx_ch=a[-1]-a[-2]; macro += 8 if tnx_ch<-.05 else -8 if tnx_ch>.05 else 0; out['sources']['US10Y']=True
    except Exception as e: out['sources']['US10Y']=False; out['errors'].append('US10Y')
    macro=clamp(macro)
    safety=100; risks=[]
    if abs(btc24)>5: safety-=18; risks.append('BTC yüksek volatilite')
    if rr>78: safety-=12; risks.append('RSI aşırı alım')
    if rr<28: safety-=10; risks.append('RSI aşırı zayıf')
    if funding is not None and abs(funding)>.001: safety-=15; risks.append('Funding aşırı')
    if oi_ch is not None and abs(oi_ch)>.05: safety-=15; risks.append('OI hızlı değişiyor')
    if spread is not None and spread>.002: safety-=12; risks.append('Spread geniş')
    if abs(tech-market)>45: safety-=10; risks.append('Teknik/piyasa çelişkisi')
    available=sum(1 for k,v in out['sources'].items() if v)
    if available<10: safety-=20; risks.append('Veri kaynağı eksik')
    safety=clamp(safety)
    intel=.30*exscore+.25*news_score+.20*onchain+.25*macro
    # Master score remains broad, while hit-and-run mode uses 15m timing as an entry filter.
    master=round(clamp(.20*tech+.10*market+.15*futures+.07*micro+.08*safety+.20*intel+.20*short))
    # Conservative gate
    gate=[]
    if available<10: gate.append('VERİ EKSİK')
    if safety<45: gate.append('RİSK FRENİ')
    if abs(tech-market)>55: gate.append('MOTOR ÇELİŞKİSİ')
    if short<52: gate.append('15M MOMENTUM ZAYIF')
    if futures<42: gate.append('FUTURES BASKISI')
    confidence=round(clamp(100*available/len(out['sources'])))
    if gate: decision='BEKLE / FIRSAT KOLLA'
    elif master>=80 and short>=65: decision='HIZLI GİRİŞ UYGUN'
    elif master>=70 and short>=58: decision='KADEMELİ GİRİŞ / TEYİT'
    elif master>=55: decision='BEKLE / FIRSAT KOLLA'
    else: decision='UZAK DUR'
    # Dynamic hit-and-run levels based on 15m ATR, not arbitrary fixed percentages.
    entry_low=price-0.20*atr15; entry_high=price+0.10*atr15
    stop=price-0.90*atr15; tp1=price+0.80*atr15; tp2=price+1.50*atr15
    rr1=(tp1-price)/(price-stop) if price>stop else 0; rr2=(tp2-price)/(price-stop) if price>stop else 0
    exit_warning = (short < 45 or futures < 40 or safety < 50)
    out.update({
      'price':price,'btc':bprice,'btc24':btc24,'master':master,'decision':decision,'confidence':confidence,
      'engines':{'technical':round(tech),'market':round(market),'futures':round(futures),'micro':round(micro),'safety':round(safety),'intel':round(intel),'exchanges':round(exscore),'news':round(news_score),'onchain':round(onchain),'macro':round(macro),'short15m':round(short)},
      'technical':{'ema20':e20,'ema50':e50,'ema200':e200,'rsi':rr,'macd':m,'volumeRatio':volratio,'bollMid':bm,'bollUpper':bup,'bollLower':blow,'atr1h':atr1h,'adx1h':adx1h,'vwap1h':vwap1h,'rsi15m':r15,'ema15m':e15,'ema15m50':e15f,'atr15m':atr15},
'chartData':[{'t':int(z[0]),'o':float(z[1]),'h':float(z[2]),'l':float(z[3]),'c':float(z[4]),'v':float(z[5]),'e20':ema(xv[:i+1],20),'e50':ema(xv[:i+1],50),'e200':ema(xv[:i+1],200),'bbm':boll(xv[:i+1])[0],'bbu':boll(xv[:i+1])[1],'bbl':boll(xv[:i+1])[2]} for i,z in list(enumerate(xk))[-160:]],
      'tradePlan':{'entryLow':entry_low,'entryHigh':entry_high,'stop':stop,'tp1':tp1,'tp2':tp2,'rr1':rr1,'rr2':rr2,'exitWarning':exit_warning},
      'futures':{'funding':funding,'openInterest':oi_val,'oiChange':oi_ch,'longShort':ls_val,'takerRatio':taker_ratio,'derivativesConsensus':derivatives_consensus,'derivatives':derivatives,'derivativesHealth':deriv_health},
      'micro':{'bookRatio':book_ratio,'spread':spread,'markGap':mark_gap},
      'intel':{'exchangePrices':ex,'exchangeSpread':exspread,'newsCount':news_count,'headlines':headlines,'xrplTx':tx_count,'largeXrpTransfers':large_count,'dxyChange':dxy_ch,'us10yChange':tnx_ch,'derivativeHealth':deriv_health},
      'risks':risks,'gates':gate
    })
    return out

def backtest(days=365):
    # Uses hourly XRP candles. Conservative signal: EMA20/50 + RSI + MACD + volume; no future leakage.
    limit=min(1000,max(100,days*24//1))
    data=klines('XRPUSDT','1h',limit)
    closes=[float(x[4]) for x in data]; highs=[float(x[2]) for x in data]; lows=[float(x[3]) for x in data]; vols=[float(x[5]) for x in data]
    equity=1.0; peak=1.0; maxdd=0; wins=losses=trades=0; pos=None; entry=0; stop=0; target=0; start=max(60,50)
    for i in range(start,len(closes)):
        v=closes[:i+1]; e20=ema(v,20); e50=ema(v,50); rr=rsi(v); m=ema(v,12)-ema(v,26); avg=sum(vols[i-21:i])/20 if i>=21 else vols[i]; vr=vols[i]/avg if avg else 1
        signal=(closes[i]>e20 and e20>e50 and 52<=rr<=70 and m>0 and vr>0.8)
        if pos is None and signal:
            entry=closes[i]; stop=entry*0.97; target=entry*1.06; pos='long'; trades+=1
        elif pos:
            # intrabar stop first = conservative
            if lows[i]<=stop:
                ret=stop/entry-1; equity*=1+ret; losses+=1 if ret<0 else 0; wins+=1 if ret>0 else 0; pos=None
            elif highs[i]>=target:
                ret=target/entry-1; equity*=1+ret; wins+=1; pos=None
        peak=max(peak,equity); maxdd=max(maxdd,1-equity/peak)
    if pos:
        ret=closes[-1]/entry-1; equity*=1+ret; wins+=ret>0; losses+=ret<=0
    return {'periodHours':len(data),'trades':trades,'wins':int(wins),'losses':int(losses),'winRate':round(100*wins/max(1,wins+losses),2),'returnPct':round((equity-1)*100,2),'maxDrawdownPct':round(maxdd*100,2),'profitFactor':'n/a (simple 1-position model)','note':'Bu backtest yalnızca teknik prototiptir; futures/news/on-chain/makro geriye dönük senkronize edilmemiştir.'}

class Handler(SimpleHTTPRequestHandler):
    def __init__(self,*args,**kwargs): super().__init__(*args,directory=BASE,**kwargs)
    def send_json(self,obj,code=200):
        b=json.dumps(obj,ensure_ascii=False).encode(); self.send_response(code); self.send_header('Content-Type','application/json; charset=utf-8'); self.send_header('Content-Length',str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        p=urlparse(self.path)
        if p.path in ('/','/index.html'):
            self.path='/index_v8.html'
            return super().do_GET()
        if p.path=='/api/snapshot':
            try:self.send_json(build_snapshot())
            except Exception as e:self.send_json({'error':str(e)},502)
        elif p.path=='/api/backtest':
            try:self.send_json(backtest(int(parse_qs(p.query).get('days',['365'])[0])))
            except Exception as e:self.send_json({'error':str(e)},502)
        else: super().do_GET()
    def log_message(self,fmt,*args): print('%s - %s'%(self.address_string(),fmt%args))

if __name__=='__main__':
    print(f'XRP AI Terminal: http://{HOST}:{PORT}')
    print('Gercek emir KAPALI. API anahtari gerektirmez.')
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
