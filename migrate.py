# -*- coding: utf-8 -*-
import couchdb, time
import json, re
import urllib.request, urllib.parse
from geotext import GeoText
from sqlalchemy import *


def removeUmlaut(my_string):
    mapping = [('Ö', 'Oe'), ('Ä', 'Ae'), ('Ü', 'Ue'), ('ö', 'oe'), ('ä', 'ae'), ('ü', 'ue')]
    for k, v in mapping:
        my_string = my_string.replace(k, v)
    return my_string

def addUmlaut(my_string):
    mapping = [('Ö', 'Oe'), ('Ä', 'Ae'), ('Ü', 'Ue'), ('ö', 'oe'), ('ä', 'ae'), ('ü', 'ue')]
    for k, v in mapping:
        my_string = my_string.replace(v, k)
    return my_string

def mapMonths(my_string):
    mapping = [('Jan. ', '1.'),
               ('Feb. ', '2.'),
               ('Febr. ', '2.'),
               ('März ', '3.'),
               ('April ', '4.'),
               ('Mai ', '5.'),
               ('Juni ', '6.'),
               ('Juli ', '7.'),
               ('Aug. ', '8.'),
               ('Sept. ', '9.'),
               ('Okt. ', '10.'),
               ('Nov. ', '11.'),
               ('Dez. ','12.')]
    for k, v in mapping:
        my_string = my_string.replace(k, v)
    return my_string

def decodeInstitutions(my_string):
    mapping = [('JE', 'Japanische Eisenbahn (Nihon Tetsudô 日本鉄道)'),
               ('JKR', 'Japanisches Rotes Kreuz (Nihon Sekijûji 日本赤十字)'),
               ('KG', 'Aktiengesellschaft (Kabushishiki Gaisha株式会社)'),
               ("LB","Lokale Behörde (chihô seifu 地方政府)"),
               ("MfA","Ministerium für Auswärtiges (Gaimushô 外務省)"),
               ("MfI","Ministerium für Inneres (Naimushô 内務省)"),
               ("MfB","Ministerium für Bildung und Kultur (Monbushô 文部省)"),
               ("MfF","Ministerium für Finanzen (Ôkurashô 大蔵省)"),
               ("MfJ","Ministerium für Justiz (Shihôshô 司法省)"),
               ("MfLH","Ministerium für Landwirtschaft und Handel (Nôshômushô 農商務省)"),
               ("MfK","Ministerium für Kaiserlichen Haushalt (Kunaishô 宮内省)"),
               ("MfKom","Ministerium für Kommunikation (Tsûshinshô 通信省)"),
               ("MfN","Ministerium für Nachrichtenwesen oder Kommunikation (Tsûshinshô 通信省)"),
               ("RI","Regierungsinstitution (nicht näher definiert)"),
               ("SME","Südmandschurische Eisenbahn (Mantetsu 満鉄)")]
    for k, v in mapping:
        my_string = my_string.replace(k, v)
    return my_string


def get_years(str):
    year_regex = r"(1[0-9]{3}/[0-9]{2}|1[0-9]{3})"
    years = re.findall(year_regex, str)
    year_start = 2000
    year_end = 0
    for year in years:
        if "/" in year:
            tmp_year1 = int(year.split("/")[0])
            tmp_year2 = int(year[0:2]+year.split("/")[1])
            if year_start > tmp_year1:
                year_start = tmp_year1
            if year_end < tmp_year2:
                year_end = tmp_year2
        else:
            if year_start > int(year):
                year_start = int(year)
            if year_end < int(year) and int(year) < 1930:
                year_end = int(year)
    if year_start == 2000:
        year_start = None
    if year_end == 0:
        year_end = None
    return [year_start, year_end]


def get_institution(str):
    institution_regex = r"((TH|U|BA) [A-Z]\w\w*)"
    res = re.search(institution_regex,str)
    if res is not None:
        return res.group(1).replace("U ", "Universität ").replace("TH ", "Technische Hochschule ")
    else:
        return None


def get_cities(str):
    places = GeoText(str.replace("U ", "u ").replace("TH ", "th "))
    if len(places.cities) > 0:
        return places.cities[0]
    else:
        return None

def get_addresses(str):
    str = removeUmlaut(str)
    addresses_regex = r'([A-Z][a-z][a-z][a-z]+[ |A-Za-z]+(\.|gasse|weg|strasse|platz| Masch|ufer| Str.) ([0-9]+/[0-9]+|[0-9]+))'
    res = re.search(addresses_regex, str)

    address_today_regex = r'\(heute: (.*)\)'
    res2 = re.search(address_today_regex, str)

    if res is not None:
        return res.group(1)
    else:
        return None


def search_nominatim(address, city):
    if city is None:
        city = "germany"
    if address is None:
        address = ""
    path = urllib.parse.quote(city.replace(" ","+")+","+address.replace(" ","+"))
    url = "http://nominatim.openstreetmap.org/search?format=json&q={}".format(path)
    req = urllib.request.Request(url)
    r = urllib.request.urlopen(req).read()
    time.sleep(5)
    result = json.loads(r.decode('utf-8'))
    if len(result) > 1:
        return {
            "lan": result[0]["lat"],
            "lon": result[0]["lon"]
        }
    else:
        return None

i=0


#connecting to local couchdb
couch = couchdb.Server()
couch.delete("ryugaku")
db = couch.create("ryugaku")



#regular expressions for finding years, institutions, and disciplines
year_regex = r"(1[0-9]{3}/[0-9]{2}|1[0-9]{3})"
institution_regex = r"[\d|?|-|–] \(([A-Z]\w*)\)"
disciplines_regex = r"[\d|\)|-|?|–] ([A-Z]\w{3}\w*)"

disciplines_array = []

#connect to mysql database
engine = create_engine('', pool_recycle=3600)
connection = engine.connect()

result = engine.execute("select name, date, city, text from jade_person")
for row in result:

    name = row["name"]
    city = row["city"]
    date = mapMonths(row["date"])
    text = row["text"]


    #getting date of birth and date of death
    if date == "-" or date == "":
        date_of_birth = None
        date_of_death = None
    else:
        date_split = date.split("–")
        if len(date_split) == 2:
            if date_split[0] == "":
                date_of_birth = None
            else:
                date_of_birth = date_split[0]
            if date_split[1] == "":
                date_of_death = None
            else:
                date_of_death = date_split[1]


    #getting first infotext line
    infotext = row["text"].replace("\r\n","").split("♦")
    first = infotext[0]
    first = first.split(" - ")[0].split(" – ")[0].replace ("U ", "u ").replace("TH ", "th")


    #identify sending institution
    institution_result = re.findall(institution_regex, first.replace("Amee", "Armee"))
    if len(institution_result) > 0:
        sending_institution = decodeInstitutions(institution_result[0])
    else:
        sending_institution = None


    #identify disciplines
    results = re.findall(disciplines_regex, first.replace("NW","Naturwissenschaften"))
    if len(results) > 0:
        discipline = results[0]
        if "Agrar" in results[0]:
            discipline = "Agrarstudien"
        if "Medizin" in results[0]:
            discipline = "Medizin"
        if "Musik" in results[0]:
            discipline = "Musikstudien"
        if "Forst" in results[0]:
            discipline = "Forstwirtschaft"
        if "Inge" in results[0]:
            discipline = "Ingenieurwesen"
        if "Recht" in results[0]:
            discipline = "Recht"
        if "Mili" in results[0]:
            discipline = "Militärstudien"
        if "Geschi" in results[0]:
            discipline = "Geschichte"
        if "Theolog" in results[0]:
            discipline = "Theologie"
        if "Kunst" in results[0]:
            discipline = "Kunststudien"
        if "Literatur" in results[0]:
            discipline = "Literaturwissenschaft"
        if "Monat" in results[0] or "Jahr" in results[0]:
            discipline = None
        if "Sprach" in results[0] or "Deutsch" in results[0]:
            discipline = "Sprachstudium"
        if "Studien" in results[0]:
            discipline = "None"
        if "Volkswirtschaft" in results[0]:
            discipline = "Nationalökonomie"
        if "Studium der deutschen" in first:
            discipline = "Sprachstudium"
        if "Chemie" in results[0]:
            discipline = "Chemie"
        if "Bau" in results[0]:
            discipline = "Bauwesen"
        if discipline not in disciplines_array:
            disciplines_array.append(discipline)
    else:
        discipline = None


    #find starting year and ending year of stay in germay
    years = re.findall(year_regex, first[:30])
    year_start = 2000
    year_end = 0
    for year in years:
        if "/" in year:
            tmp_year1 = int(year.split("/")[0])
            tmp_year2 = int(year[0:2]+year.split("/")[1])
            if year_start > tmp_year1:
                year_start = tmp_year1
            if year_end < tmp_year2:
                year_end = tmp_year2
        else:
            if year_start > int(year):
                year_start = int(year)
            if year_end < int(year):
                year_end = int(year)

    #print(name)
    #print("  "+str(year_start)+ " - "+str(year_end))

    abschlussarbeit_regex = r"Dr\..*(1[0-9]{3}),(.*): (.+?)\..*S\."

    res = re.search(abschlussarbeit_regex,text)
    dr_thesis = None
    if res is not None:
        dr_thesis_year = res.group(1).strip()
        dr_thesis_town = res.group(2).strip()
        dr_thesis_title = res.group(3).strip()

        dr_thesis = {"year": dr_thesis_year,
                     "town": dr_thesis_town,
                     "title": dr_thesis_title}

    details = []

    older_city = None

    for line in infotext:

        years = get_years(line)
        cities = get_cities(line)
        institution = get_institution(line)
        addresses = get_addresses(line)
        #print(addresses)

        if cities is None:
            if institution != None:
                cities = institution.split(" ")[-1]
            else:
                cities = older_city

        #print(institution)
        #print(li
        coord = None

        if addresses is not None:
            print(addresses)
            print(line)
            #coord = search_nominatim(addresses, cities)
            print(coord)

        if cities is not None and cities != older_city:
         older_city = cities

        if cities is not None or institution is not None or addresses is not None:
            details.append({"year_start" : years[0],
                            "year_end" : years[1],
                            "city": cities,
                            "address": { "literal": addresses,
                                         "coord": coord },
                            "institution": institution})
    #print(name)

    if len(details) == 0:
        details = None

    print(i)
    i += 1
    #writing docs in couchdb
    person = {"name": name,
              "place_of_origin": city,
              "sending_institution": sending_institution,
              "date_of_birth": date_of_birth,
              "date_of_death": date_of_death,
              "discipline": discipline,
              "year_start": year_start,
              "year_end":year_end,
              "dr_thesis": dr_thesis,
              "details": details,
              "text": text.replace("\r\n"," "),
              "checked": False}
    db.save(person)


    places = GeoText(row["text"].splitlines()[0].replace("U ", "u ").replace("TH ", "th "))
    #print(places.cities)

result.close()
