import requests
import io
import xml.etree.ElementTree as ET
import psycopg2
import datetime


def getURLfromSitemaps(sitemapurl):

    headers = {
        'user-agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
    }
    while(True):
        try:
            response = requests.get(sitemapurl, headers=headers)
            data = response.text
            tree = ET.parse(io.StringIO(data))
            root = tree.getroot()
            listt = []
            for url in root.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
                # listt.append(url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text)
                sitemap = []
                sitemap.append(url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text)
                sitemap.append(url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod').text)
                # print("'"+url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text+"',"+url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod').text)
                listt.append(sitemap)
            break
        except Exception as e:
            print("retry to get sitemap from url: "+sitemapurl)
    return listt


def saveSitemap(sitemaps):
    cur.execute('TRUNCATE TABLE "Alex".sitemap;')
    conn.commit()
    for sitemap in sitemaps:
        cur.execute('INSERT INTO "Alex".sitemap VALUES(%s, %s) ON CONFLICT DO NOTHING;', (sitemap[0], sitemap[1]))
        conn.commit()


def compareAndRecord(sitemaps):
    cur.execute('SELECT url FROM "Alex".sitemap;')
    rows = cur.fetchall()
    oldsitemap = []
    newsitemap = []
    insertsitemap = []
    deletesitemap = []
    for row in rows:
        oldsitemap.append(row[0])
    for sitemap in sitemaps:
        newsitemap.append(sitemap[0])

    for sitemap in oldsitemap:
        if sitemap not in newsitemap:
            deletesitemap.append(sitemap)
    for sitemap in newsitemap:
        if sitemap not in oldsitemap:
            insertsitemap.append(sitemap)


    for sitemap in insertsitemap:
        cur.execute('INSERT INTO "Alex".new_url VALUES(%s,%s);', (sitemap, datetime.date.today()))
        conn.commit()

    for sitemap in deletesitemap:
        cur.execute('INSERT INTO "Alex".delete_url VALUES(%s,%s);', (sitemap, datetime.date.today()))
        conn.commit()


if __name__ == '__main__':
    conn = psycopg2.connect(database='seo',user='edwin',password='sacred-bird-spread-lord',
                            host='seo.cooqskzbn9a2.ap-southeast-1.rds.amazonaws.com',port='5432')
    cur = conn.cursor()

    list1 = getURLfromSitemaps('https://www.docdoc.com/sitemap-document_translations.xml')
    list2 = getURLfromSitemaps('https://www.docdoc.com/sitemap-providers.xml')
    list3 = getURLfromSitemaps('https://www.docdoc.com/sitemap-providerIds.xml')
    sitemaps = list1+list2+list3

    compareAndRecord(sitemaps)

    saveSitemap(sitemaps)

    cur.close()
    conn.close()