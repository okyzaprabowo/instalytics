from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.firefox.options import Options

from InstagramAPI import InstagramAPI
from bs4 import BeautifulSoup

import pymysql
import tzlocal
import json
import time
import datetime
import os

average_scrapping_time = []
target = []
scrapingFailed = []
config = None

# reading configuration json
dir_path = os.path.dirname(os.path.realpath(__file__))
with open(dir_path + '/config.json') as json_file:
    config = json.load(json_file)

# Open database connection
db = pymysql.connect(host=config['database']['host'],
                     user=config['database']['username'],
                     password=config['database']['password'],
                     db=config['database']['databasename'],
                     charset='utf8mb4',)

# prepare a cursor object using cursor() method
cursor = db.cursor()

# define engine id and crawling cycle
id_engine = config['id_engine']
crawling_cycle = config['crawling_cycle']
custom_cycle_num_days = config['custom_cycle_num_days']
print(crawling_cycle, custom_cycle_num_days)

# set up firefox driver
options = Options()
options.headless = True
options.add_argument("--private")
driver = webdriver.Firefox(
    options=options, executable_path=config['geckodriver_path'])

actions = ActionChains(driver)


# Database Logic
def saveToDatabase(data):
    try:
        with db.cursor() as cursor:
            # select first for same ID
            sql = "SELECT ig_username, url, comment_count, response_count FROM `tbl_scraping` WHERE `url`=%s"
            cursor.execute(sql, (data['url']))
            result = cursor.fetchone()

            if((result == None)):
                # Create a new record
                try:
                    sql = "INSERT INTO `tbl_scraping` (`ig_username`, `url`, `follower_count`, `like_count`, `comment_count`, `response_count`, `taken_at`, `completed`, `category`, `updated`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, (data['ig_username'], data['url'], data['follower_count'],
                                         data['like_count'], data['comment_count'], data['response_count'], data['taken_at'], data['completed'], data['category'], datetime.datetime.now()))
                    # print(sql)
                    print('{} is saved'.format(data['url']))
                except pymysql.InternalError as e:
                    print('Got error {!r}, errno is {}'.format(e, e.args[0]))

            else:
                total_comment = result[2] + result[3]
                total_new_scraping_comment = data['comment_count'] + data['response_count']
                if(total_new_scraping_comment > total_comment):
                    # Update data
                    try:
                        sql = "UPDATE `tbl_scraping` SET `comment_count` = %s, `response_count` = %s, `completed` = %s WHERE `url` = %s"
                        cursor.execute(
                            sql, (data['comment_count'], data['response_count'], data['completed'], data['url']))
                        # print(sql)
                        print('{} is updated'.format(data['url']))
                    except pymysql.InternalError as e:
                        print('Got error {!r}, errno is {}'.format(
                            e, e.args[0]))
                else:
                    sql = "UPDATE `tbl_scraping` SET `completed` = %s WHERE `url` = %s"
                    cursor.execute(sql, (data['completed'], data['url']))

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        db.commit()
    except pymysql.InternalError as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))


# scraping Comments Logic
def scrappingComments(post_url, post_username):
    start_datetime = datetime.datetime.now()
    completed = 1
    driver.get(post_url)

    # load more comments until the end
    click_count = 0
    while (1):
        try:
            # wait load more button to appear
            load_more = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, config['scraper_var']['load_comment_icon_class_name']))
            )
        except Exception as e:
            # load more button not appear
            print(e)
            # check if stuck in loading problem
            if(driver.find_elements_by_class_name(config['scraper_var']['loading_svg_class_name'])):
                # set to uncompleted
                completed = 0
            break

        # click load more button
        driver.execute_script("arguments[0].click();", load_more)
        click_count += 1
        print("btn clicked ("+str(click_count)+")")

    # load more reply comments
    click_reply_count = 0
    while(1):
        try:
            # load another reply in the same section until clear
            load_another_reply_count = 1
            is_load_more_reply = True
            while(is_load_more_reply == True):
                # use this to terminate the process, in case of problem
                need_to_terminate = False

                load_reply = driver.find_elements_by_class_name(
                    config['scraper_var']['load_comment_reply_span_class_name'])
                if(load_reply[click_reply_count].text == config['scraper_var']['end_load_comment_reply_text'] or load_reply[click_reply_count].text == config['scraper_var']['end_load_comment_reply_text_id']):
                    # clear hit until the end
                    print("all clear")
                    is_load_more_reply = False
                else:
                    # click another reply
                    print("load another reply clicked (" +
                          str(load_another_reply_count)+")")
                    driver.execute_script(
                        "arguments[0].click();", load_reply[click_reply_count])
                    load_another_reply_count += 1

                    try:
                        # wait loading icon until disappear
                        load_more = WebDriverWait(driver, 15).until_not(
                            EC.presence_of_element_located(
                                (By.CLASS_NAME, config['scraper_var']['loading_reply_svg_class_name']))
                        )
                    except Exception as e:
                        # loading icon not disappear
                        completed = 0
                        need_to_terminate = True
                        break
                        # completed = 0

            click_reply_count += 1

            # terminate if True
            if(need_to_terminate):
                break

        except Exception as e:
            print(e)
            break

    # get the scraping results
    page_source = driver.page_source

    soup = BeautifulSoup(page_source, 'html.parser')
    comment_count = 0
    response_count = 0
    print("=====================================================================")
    for row in soup.select(config['scraper_var']['comment_content_container_selector']):
        if(row.find('h3')):
            username_comment = row.find('h3').find('a').get_text()
            comment_count += 1
            if(username_comment == post_username):
                response_count += 1
            print("["+str(username_comment)+"]")
    print("=====================================================================")

    comment_count = comment_count - response_count

    print("comments total: "+str(comment_count))
    print("respons total: "+str(response_count))

    end_datetime = datetime.datetime.now()
    total_second = (end_datetime - start_datetime).seconds
    average_scrapping_time.append(
        {'comments': comment_count+response_count, 'time': total_second})

    # set the response
    response = {'comment_count': comment_count,
                'response_count': response_count, 'completed': completed}
    return response


def calculateApiEngagementRange(username):
	print("Calculating engagement range for ", username)
	results = []
	InstagramAPI.searchUsername(username)
	user_data = InstagramAPI.LastJson
	userpk = user_data['user']['pk']
	InstagramAPI.getUserFeed(userpk)
	data = InstagramAPI.LastJson
	post_likers = []
	for item in data['items']:
		dataLike = item['like_count']
		post_likers.append(dataLike)
		print('Likers ', item['like_count'])
		print('https://instagram.com/p/', item['code'])
	post_likers.sort()
	highest = post_likers[len(data['items'])-1]
	# Searching for spike
	spike_rate = highest / post_likers[len(data['items'])-2]
	if(spike_rate > 2):
		del post_likers[-1]
		highest = post_likers[len(data['items'])-2]
	roundup = (highest - post_likers[0]) / 4
	results.append(roundup)
	results.append(post_likers[0])
	print(roundup, " - hasil dari ", post_likers)
	return results


def crawlsUserFeed(username, results):
    InstagramAPI.searchUsername(username)
    data = InstagramAPI.LastJson
    userpk = data['user']['pk']
    print('Searching posts data with username :', username, ' (', userpk, ')')
    dataFollower = data['user']['follower_count']
    print('Total Follower: ', dataFollower)
    # avgLike = calculateEngagementRange(data['items'])
    next_max_id = ''
    loop_state = True
    now = datetime.datetime.now()
    now_today = now.strftime("%d")
    while loop_state:
        InstagramAPI.getUserFeed(userpk, next_max_id, None)
        temp = InstagramAPI.LastJson
        for item in temp["items"]:
            url = "https://instagram.com/p/" + item['code']
            ts = int(item['taken_at'])
            local_timezone = tzlocal.get_localzone()  # get pytz timezone
            taken_at_local_time = datetime.datetime.fromtimestamp(
                ts, local_timezone)
            taken_at_day = taken_at_local_time.strftime("%d")
            # now = datetime.datetime.now()
            # now_today = now.strftime("%d")
            # # yesterday = now + datetime.timedelta(days=-1)
            # # now_yesterday = yesterday.strftime("%d")

            date_format = "%Y/%m/%d"
            taken_at_local_time_formatted = datetime.datetime.strptime(
                taken_at_local_time.strftime(date_format), date_format)
            now_formatted = datetime.datetime.strptime(
                now.strftime(date_format), date_format)
            datediff_now_taken_at = now_formatted - taken_at_local_time_formatted

            if(crawling_cycle == 'daily'):
                if(datediff_now_taken_at.days != 1 and now_today != taken_at_day):
                    loop_state = False
                    break
            elif(crawling_cycle == 'weekly'):
                if((datediff_now_taken_at.days <= 0 or datediff_now_taken_at.days > 7) and now_today != taken_at_day):
                    loop_state = False
                    break
            elif(crawling_cycle == 'monthly'):
                if((datediff_now_taken_at.days <= 0 or datediff_now_taken_at.days > 30) and now_today != taken_at_day):
                    loop_state = False
                    break
            elif(crawling_cycle == 'yearly'):
                if((datediff_now_taken_at.days <= 0 or datediff_now_taken_at.days > 365) and now_today != taken_at_day):
                    loop_state = False
                    break
            elif(crawling_cycle == 'custom'):
                if((datediff_now_taken_at.days <= 0 or datediff_now_taken_at.days > int(custom_cycle_num_days)) and now_today != taken_at_day):
                    loop_state = False
                    break
            else:
                print("Crawling cycle is not in list!")
                loop_state = False
                break

            if(now_today != taken_at_day and loop_state == True):
                dataLike = item['like_count']

                # calculate engagement
                engagement = 'L'
                r1 = results[1] + results[0]
                r2 = r1 + results[0]
                r3 = r2 + results[0]
                if((dataLike >= results[1]) and (dataLike < r1)):
                    engagement = 'L'	
                elif((dataLike >= r1) and (dataLike < r2)):
                    engagement = 'M'	
                elif((dataLike >= r2) and (dataLike < r3)):
                    engagement = 'H'	
                elif(dataLike >= r3):
                    engagement = 'H'	

                # scraping comments
                scraping_result = scrappingComments(url, username)

                # ready to save
                saveData = {
                    'ig_username': username,
                    'url': url,
                    'follower_count': dataFollower,
                    'like_count': dataLike,
                    'comment_count': scraping_result['comment_count'],
                    'response_count': scraping_result['response_count'],
                    'taken_at': taken_at_local_time,
                    'completed': scraping_result['completed'],
                    'category': engagement
                }

                saveToDatabase(saveData)

        if temp["more_available"] is False:
            pass
        next_max_id = temp["next_max_id"]


# scrap igtv post
def scrapIgtvPosts(username, results):
    print("scrap igtv post")
    postUrlList = []
    pageUrl = 'https://www.instagram.com/' + username + '/channel/'
    followerCount = 0
    driver.get(pageUrl)
    
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CLASS_NAME, "_bz0w"))
        )

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        followerCount = int(soup.select("span.g47SY")[1]['title'].replace(",", "").replace(".", ""));

        i = 0
        for row in soup.select("a._bz0w"):
            i += 1
            postUrlList.append(row['href'])
            if (i >= 15):
                break

    except Exception as e:
        # load more button not appear
        print(e)

    for url in postUrlList:
        postUrl = 'https://www.instagram.com/' + url
        driver.get(postUrl)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CLASS_NAME, "_1o9PC.Nzb55"))
        )

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        time = soup.select("time._1o9PC.Nzb55")
        postDatetime = time[0]['datetime']
        postDate = postDatetime[0:10]

        yesterdayDate = datetime.date.today() - datetime.timedelta(days=1)
        yesterdayDate = yesterdayDate.strftime('%Y-%m-%d')

        if(str(postDate) == str(yesterdayDate)):
            likeCount = int(soup.select("span.vcOH2")[0].find("span").get_text().replace(",", "").replace(".",""))

            # calculate engagement
            dataLike = likeCount
            engagement = 'L'
            r1 = results[1] + results[0]
            r2 = r1 + results[0]
            r3 = r2 + results[0]
            if((dataLike >= results[1]) and (dataLike < r1)):
                engagement = 'L'	
            elif((dataLike >= r1) and (dataLike < r2)):
                engagement = 'M'	
            elif((dataLike >= r2) and (dataLike < r3)):
                engagement = 'H'	
            elif(dataLike >= r3):
                engagement = 'H'

            # scraping comments
            scraping_result = scrappingComments(postUrl, username)

            # ready to save
            saveData = {
                'ig_username': username,
                'url': postUrl,
                'follower_count': followerCount,
                'like_count': likeCount,
                'comment_count': scraping_result['comment_count'],
                'response_count': scraping_result['response_count'],
                'taken_at': postDatetime,
                'completed': scraping_result['completed'],
                'category': engagement
            }

            saveToDatabase(saveData)


# Start Program
# get targets username
try:
    # Execute the SQL command
    cursor.execute(
        "SELECT tg.ig_username FROM tbl_scrap_targets tg INNER JOIN tbl_engines en ON en.id_engine = tg.id_engine WHERE en.id_engine = '"+str(id_engine)+"'")
    # Fetch all the rows in a list of lists.
    results = cursor.fetchall()

    for row in results:
        target_username = row[0]
        # Now print fetched result
        target.append(target_username)
except:
    print("Error: unable to fetch data")

# # get client username
# try:
#     # Execute the SQL command
#     cursor.execute(
#         "SELECT us.ig_username FROM tbl_users us INNER JOIN tbl_engines en ON en.id_engine = us.id_engine WHERE en.id_engine = '"+str(id_engine)+"'")
#     # Fetch all the rows in a list of lists.
#     results = cursor.fetchall()

#     for row in results:
#         target_username = row[0]
#         # Now print fetched result
#         target.append(target_username)
# except:
#     print("Error: unable to fetch data")

print(target)


# start crawling and scraping comments
try:
    # Login
    cursor.execute(
        "SELECT * FROM tbl_engines eng WHERE eng.id_engine = '"+str(id_engine)+"'")
    result = cursor.fetchone()

    if (result != None):

        # # login to ig first
        # driver.get("https://www.instagram.com/accounts/login/")
        # try:
        #     login_btn = WebDriverWait(driver, 15).until(
        #         EC.presence_of_element_located(
        #             (By.CLASS_NAME, config['scraper_var']['login_button_class_name']))
        #     )
        # except Exception as e:
        #     print(e)

        # driver.find_element_by_xpath(
        #     config['scraper_var']['login_input_username_xpath']).send_keys('dimasadiandrea')
        # driver.find_element_by_xpath(
        #     config['scraper_var']['login_input_password_xpath']).send_keys('secret')
        # driver.execute_script("arguments[0].click();", login_btn)


        InstagramAPI = InstagramAPI(result[1], result[2])
        InstagramAPI.login()
        # Get username and pk info
        for target_username in target:
            try:
                print("==========START==========")
                calculation_result = calculateApiEngagementRange(target_username)
                crawlsUserFeed(target_username, calculation_result)
                scrapIgtvPosts(target_username, calculation_result)
                print("===========END===========")
            except Exception as e:
                print(e)

        print("==========START CALCULATION==========")
        # calc.execute()
        print("===========END CALCULATION===========")

        # print(average_scrapping_time)
        # comments_total = 0
        # time_total = 0
        # for avg in average_scrapping_time:
        #     comments_total += avg['comments']
        #     time_total += avg['time']
        # comments_per_second = comments_total/time_total
        # print(str(comments_total) + ' comments')
        # print(str(time_total) + ' seconds')
        # print(str(comments_per_second) + ' comments per second')

except KeyboardInterrupt:
    print("Stopped by user")


# # re-scrap the failed ones
# try:
#     print("trying re-scrape the failed ones one more time")
#     cursor.execute("SELECT * FROM tbl_scraping WHERE completed = 0")
#     # Fetch all the rows in a list of lists.
#     results = cursor.fetchall()
#     for row in results:

#         username = row[1]
#         url = row[2]
#         dataFollower = row[3]
#         dataLike = row[4]
#         taken_at = row[7]

#         print(username+" "+url)

#         # scraping comments
#         scraping_result = scrappingComments(url, username)
#         if(scraping_result['completed'] == 0):
#             completed = 2
#         else:
#             completed = 1

#         # ready to save
#         saveData = {
#             'ig_username': username,
#             'url': url,
#             'follower_count': dataFollower,
#             'like_count': dataLike,
#             'comment_count': scraping_result['comment_count'],
#             'response_count': scraping_result['response_count'],
#             'taken_at': taken_at,
#             'completed': completed
#         }
#         saveToDatabase(saveData)

# except KeyboardInterrupt:
#     print("Stopped by user")


driver.close()