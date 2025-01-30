# cpa_utils.py

import math

METER_PER_NM = 1852.0

def local_distance_nm(shipA, shipB):
    """
    Oblicza dystans w milach morskich w przybliżonej projekcji lokalnej:
      x = lon * scaleLon
      y = lat * scaleLat
    Wykorzystujemy średnią szerokość (lat_ref) do obliczenia skali.
    
    :param shipA: dict zawierający 'latitude' i 'longitude'
    :param shipB: dict zawierający 'latitude' i 'longitude'
    :return: dystans w nm (float)
    """
    lat_ref = (shipA['latitude'] + shipB['latitude']) / 2.0
    scale_lat = 111000.0
    scale_lon = 111000.0 * math.cos(math.radians(lat_ref))

    xA = shipA['longitude'] * scale_lon
    yA = shipA['latitude']  * scale_lat
    xB = shipB['longitude'] * scale_lon
    yB = shipB['latitude']  * scale_lat

    dist_m = math.sqrt((xA - xB)**2 + (yA - yB)**2)
    dist_nm = dist_m / METER_PER_NM
    return dist_nm

def is_approaching(shipA, shipB):
    """
    Sprawdza, czy dwa statki się zbliżają (iloczyn skalarny < 0):
      1) Wyznacz wektor różnicy położenia (ΔP).
      2) Wyznacz wektor różnicy prędkości (ΔV) w m/min (z SOG i COG).
      3) Jeśli ΔP · ΔV < 0, statki się zbliżają.
      
    :param shipA: dict z polami 'latitude','longitude','cog','sog'
    :param shipB: dict z polami 'latitude','longitude','cog','sog'
    :return: True, jeśli statki zbliżają się; False w p.p.
    """
    lat_ref = (shipA['latitude'] + shipB['latitude']) / 2.0
    scale_lat = 111000.0
    scale_lon = 111000.0 * math.cos(math.radians(lat_ref))

    xA = shipA['longitude'] * scale_lon
    yA = shipA['latitude']  * scale_lat
    xB = shipB['longitude'] * scale_lon
    yB = shipB['latitude']  * scale_lat

    dx = xA - xB
    dy = yA - yB

    def cog_to_vector(cog_deg, sog_kn):
        r = math.radians(cog_deg or 0.0)
        vx_kn = sog_kn * math.sin(r)  # nm/h
        vy_kn = sog_kn * math.cos(r)
        return vx_kn, vy_kn

    vxA_kn, vyA_kn = cog_to_vector(shipA['cog'], shipA['sog'])
    vxB_kn, vyB_kn = cog_to_vector(shipB['cog'], shipB['sog'])

    # Konwersja nm/h na m/min
    speed_scale = METER_PER_NM / 60.0
    dvx = (vxA_kn - vxB_kn) * speed_scale
    dvy = (vyA_kn - vyB_kn) * speed_scale

    dot_product = dx * dvx + dy * dvy
    return dot_product < 0.0

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
    Jeżeli dane nie są wystarczające lub TCPA < 0, zwraca (9999, -1).
    """
    required_fields = ['latitude', 'longitude', 'cog', 'sog']
    for f in required_fields:
        if f not in shipA or f not in shipB:
            return (9999, -1)
        if shipA[f] is None or shipB[f] is None:
            return (9999, -1)

    # B) Ustal lokalny punkt odniesienia (latRef)
    latRef = (shipA['latitude'] + shipB['latitude']) / 2.0

    # Skale w metrach
    scaleLat = 111000.0
    scaleLon = 111000.0 * math.cos(math.radians(latRef))

    def toXY(lat, lon):
        x = lon * scaleLon
        y = lat * scaleLat
        return (x, y)

    # C) Konwersja pozycji
    xA, yA = toXY(shipA['latitude'], shipA['longitude'])
    xB, yB = toXY(shipB['latitude'], shipB['longitude'])

    sogA_kn = float(shipA['sog'] or 0)
    sogB_kn = float(shipB['sog'] or 0)

    # D) Funkcja pomocnicza do konwersji COG-> wektor (nm/h)
    def cogToVector(cog_deg, sog_kn):
        r = math.radians(cog_deg or 0)
        vx = sog_kn * math.sin(r)  # nm/h
        vy = sog_kn * math.cos(r)
        return (vx, vy)

    vxA_kn, vyA_kn = cogToVector(shipA['cog'], sogA_kn)
    vxB_kn, vyB_kn = cogToVector(shipB['cog'], sogB_kn)

    dx = xA - xB  # metry
    dy = yA - yB  # metry

    # E) Przeliczenie prędkości z nm/h na m/min
    # 1 nm = 1852 m, 1 h = 60 min => 1 nm/h = 1852/60 m/min
    speed_scale = METER_PER_NM / 60.0

    dvx = (vxA_kn - vxB_kn) * speed_scale  # m/min
    dvy = (vyA_kn - vyB_kn) * speed_scale  # m/min

    VV = dvx**2 + dvy**2
    PV = dx * dvx + dy * dvy

    if VV == 0:
        # Statki stoją względem siebie (lub poruszają się identycznie)
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
    dist_nm = dist_m / METER_PER_NM

    return (dist_nm, tcpa)