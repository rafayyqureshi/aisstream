# cpa_utils.py

import math

def compute_cpa_tcpa(shipA, shipB):
    """
    Oblicza (cpa, tcpa) w milach morskich (nm) i minutach, 
    wykorzystując lokalny układ współrzędnych:
      - lat/lon -> (x, y) w metrach,
      - SOG (kn) -> m/min,
      - COG (deg, 0°=North, rośnie cw).

    Wymagane pola: 
       ship['latitude'], ship['longitude'], ship['cog'], ship['sog']
    Zwraca (cpa_val, tcpa_val). 
    Jeżeli dane nie są wystarczające lub TCPA <0, zwraca (9999, -1).
    """

    # A) Sprawdź czy mamy dane
    required_fields = ['latitude', 'longitude', 'cog', 'sog']
    for f in required_fields:
        if f not in shipA or f not in shipB:
            return (9999, -1)
        if shipA[f] is None or shipB[f] is None:
            return (9999, -1)

    # B) Ustal lokalny punkt odniesienia (latRef)
    latRef = (shipA['latitude'] + shipB['latitude']) / 2.0

    # Skale w metrach:
    scaleLat = 111000.0
    scaleLon = 111000.0 * math.cos(math.radians(latRef))

    def toXY(lat, lon):
        # Konwersja lat/lon -> (x, y) [metry]
        x = lon * scaleLon
        y = lat * scaleLat
        return (x, y)

    # C) Konwersja pozycji
    xA, yA = toXY(shipA['latitude'], shipA['longitude'])
    xB, yB = toXY(shipB['latitude'], shipB['longitude'])

    sogA_kn = float(shipA['sog'] or 0)  # węzły (nm/h)
    sogB_kn = float(shipB['sog'] or 0)

    # D) Funkcja pomocnicza do konwersji COG-> wektor (nm/h)
    def cogToVector(cog_deg, sog_kn):
        # cog_deg w stopniach, sog_kn w nm/h
        r = math.radians(cog_deg or 0)
        vx = sog_kn * math.sin(r)
        vy = sog_kn * math.cos(r)
        return (vx, vy)

    vxA_kn, vyA_kn = cogToVector(shipA['cog'], sogA_kn)
    vxB_kn, vyB_kn = cogToVector(shipB['cog'], sogB_kn)

    dx = xA - xB  # metry
    dy = yA - yB  # metry

    # E) Przeliczenie prędkości z nm/h na m/min
    #    1 nm = 1852 m, 1 h = 60 min => 1 nm/h = 1852/60 m/min
    speed_scale = 1852.0 / 60.0

    dvx = (vxA_kn - vxB_kn) * speed_scale  # m/min
    dvy = (vyA_kn - vyB_kn) * speed_scale  # m/min

    VV = dvx**2 + dvy**2
    PV = dx * dvx + dy * dvy

    if VV == 0:
        # Statki stoją względem siebie
        tcpa = 0.0
    else:
        tcpa = -PV / VV

    if tcpa < 0:
        # Kolizja "była w przeszłości" lub nie ma kolizji
        return (9999, -1)

    # F) Pozycja przy CPA
    xA2 = xA + vxA_kn * speed_scale * tcpa
    yA2 = yA + vyA_kn * speed_scale * tcpa
    xB2 = xB + vxB_kn * speed_scale * tcpa
    yB2 = yB + vyB_kn * speed_scale * tcpa

    dist_m = math.sqrt((xA2 - xB2)**2 + (yA2 - yB2)**2)
    dist_nm = dist_m / 1852.0  # w milach morskich

    return (dist_nm, tcpa)