from IPython.display import *
from ipywidgets import *
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import os

import pandas as pd
import re
import datetime 
from datetime import datetime


def db_connect1(driver, server, database, UID, password):
    '''
    Function to connect with the Database with the user details
    '''
    import pyodbc
    #connect with user entered values
    conn = pyodbc.connect(driver=driver, host=server, database=database, uid=UID, pwd=password)
    return conn


def db_connect2(driver, server, database, UID, password):
    import pymssql
        # To connect MySQL database
    conn = pymssql.connect(
        server=server,
        user=UID, 
        password = password,
        database=database,
        )
    return conn
try:
    conn = db_connect1(driver='ODBC Driver 17 for SQL Server', server='20.198.89.10', database='speedlabs-anon', UID='Speedlabsread', password='$tar@Night')
except:
    conn = db_connect2(driver='ODBC Driver 17 for SQL Server', server='20.198.89.10', database='speedlabs-anon', UID='Speedlabsread', password='$tar@Night')


df_course = pd.read_sql('SELECT COURSEID, COURSENAME FROM COURSE', con=conn)
df_chapter = pd.read_sql('SELECT CHAPTERID, CHAPTERNAME FROM CHAPTER', con=conn)
df_course_chapter = pd.read_sql('SELECT COURSEID, CHAPTERID FROM COURSECHAPTER', con=conn)
df_course_subject = pd.read_sql('SELECT COURSEID, SUBJECTID FROM COURSESUBJECT', con=conn)
df_course_topic = pd.read_sql('SELECT COURSEID, TOPICID FROM COURSETOPIC', con=conn)
df_subject_name = pd.read_sql('SELECT SubjectName, SubjectId FROM Subject', con = conn)
df_question_ksc = pd.read_sql('SELECT kscId, QuestionId from Questionksc', con=conn)
df_course_ksc = pd.read_sql('SELECT kscId, courseid from COURSEKSC', con=conn)
df_question_id_type = pd.read_sql('SELECT QuestionId, QuestionTypeId, _SubjectId, _QuestionScore, _TotalQuestionAppear from Question', con=conn)
df_batch = pd.read_sql('SELECT batchname, batchid, courseid from Batch', con=conn)
df_batch_student = pd.read_sql('SELECT batchid, userid from BatchStudent', con=conn)
df_user_login = pd.read_sql('SELECT userid, createdon, endon, isactive, lastaccessedtime FROM userloginSession', con=conn)
df_question_name = pd.read_sql('SELECT QuestionTypeId, QuestionType from QuestionType', con=conn)
df_video = pd.read_sql('SELECT videoid, courseid, subjectid from VIMEOVIDEO', con=conn)

def db_connection():
        
    #To connect database with user entered values
    '''
    It is for user interaction that takes details
    via widgets that can be used to connect with the Database
    '''
        
    #select the driver for your database
    Driver = Text(value='ODBC Driver 17 for SQL Server', description="driver: ")

    #select server on which it is hosted
    Server = Text(
            value='20.198.89.10',
            placeholder='Enter server',
            description='Server: ',
            disabled=False
        )

    #select database name
    Database = Text(
            value='speedlabs-anon',
            placeholder='Enter Database name',
            description='Database: ',
            disabled=False
        )

    #select user id
    UID =  Text(
            value='Speedlabsread',
            placeholder='Enter UID',
            description='UID: ',
            disabled=False
        )
   

    #select password
    password = Text(
            value='$tar@Night',
            placeholder='Enter password',
            description='password: ',
            disabled=False
        )
    return Driver, Server, Database, UID, password 
Driver, Server, Database, UID, password = db_connection()

#merge course table and ksc table   -->   there isone to many relationship between course and ksc
df_course_ksc_question = pd.merge(df_course_ksc, df_question_ksc, on='kscId', how='left')
df_course_ksc_question.dropna(inplace=True)
#remove unnecessary columns
df_course.drop([18, 19, 43, 44], axis=0, inplace=True)
#reset index after dropping unnecessary rows
df_course.reset_index(drop=True, inplace=True)



# df_course_question_subject

sns.set_palette("YlGnBu")

#map course question and subject
df_course_question_subject = pd.merge(df_question_id_type, df_course_ksc_question, on='QuestionId', how='right')
df_course_ksc_question = pd.merge(df_course_ksc, df_question_ksc, on='kscId', how='left')
df_course_question_subject = pd.merge(df_course_question_subject, df_course, left_on='courseid', right_on='COURSEID', how='left')
df_course_question_subject = pd.merge(df_course_question_subject, df_subject_name, left_on='_SubjectId', right_on='SubjectId', how='left')
df_course_question_subject = pd.merge(df_course_question_subject, df_question_name, on='QuestionTypeId', how='left')
df_course_question_subject.dropna(inplace=True)

if not os.path.exists("images"):
    os.mkdir("images")

def question_type_subset1(course, subject):
    df_final = df_course_question_subject[(df_course_question_subject.COURSENAME == course) & (df_course_question_subject.SubjectName == subject)]
    data = dict(df_final.QuestionType.value_counts()) 
    x = list(data.keys())
    y = list(data.values())
    
    fig = px.pie(values=y, names=x, title=f'It tells QuestionType distribution i.e. no of questions  with subject {subject} and course {course}')
    fig.show()
    fig.write_image("images/question_type1.pdf")



def question_type_subset2(course, subject):
    
    df_final1 = df_course_question_subject[df_course_question_subject.COURSENAME == course]
    data = dict(df_final1.QuestionType.value_counts()) 
    x = list(data.keys())
    y = list(data.values())
    fig = px.pie(values=y, names=x, title=f'QuestionType distribution i.e. no of questions with  course {course}')
    fig.show()
    fig.write_image("images/question_type2.pdf")

def question_type_subset3(course, subject):
    
    df_final3 = df_course_question_subject[df_course_question_subject.SubjectName == subject]
    data = dict(df_final3.QuestionType.value_counts()) 
    x = list(data.keys())
    y = list(data.values())
    fig = px.pie(values=y, names=x, title=f'QuestionType distribution i.e. no of questions with  SUBJECT {subject}')
    fig.show()
    fig.write_image("images/question_type3.pdf")

   
def question_accuracy(course, subject):
    df_final = df_course_question_subject[(df_course_question_subject.COURSENAME == course) & (df_course_question_subject.SubjectName == subject)]
    df_final1 = df_course_question_subject[(df_course_question_subject.COURSENAME == course)]
    df_final2 = df_course_question_subject[ (df_course_question_subject.SubjectName == subject)]
    data1 = df_final.groupby('QuestionType', as_index=False).agg(accuracy = ('_QuestionScore', 'mean'))
    data2 = df_final1.groupby('QuestionType', as_index=False).agg(accuracy = ('_QuestionScore', 'mean'))
    data3 = df_final2.groupby('QuestionType', as_index=False).agg(accuracy = ('_QuestionScore', 'mean'))
    fig = px.bar(data1, x='QuestionType', y='accuracy',
             hover_data=['QuestionType'], color='accuracy',
              height=400, title=f'It tells QuestionScore mean values i.e average score across different score types with subject {subject} and course {course}')
    fig.show()
    fig.write_image("images/question_accuracy1.pdf")
    fig = px.bar(data2,  x='QuestionType', y='accuracy',
             hover_data=['QuestionType'], color='accuracy',
              height=400, title=f'It tells QuestionScore mean values i.e average score across different score types with  course {course}')
    fig.show()
    fig.write_image("images/question_accuracy2.pdf")
    fig = px.bar(data3, x='QuestionType', y='accuracy',
             hover_data=['QuestionType'], color='accuracy',
              height=400, title=f'It tells QuestionScore mean values i.e average score across different score types with subject {subject}')
    fig.show()
    fig.write_image("images/question_accuracy3.pdf")

def question_hist(course, subject):
    df_final = df_course_question_subject[(df_course_question_subject.COURSENAME == course) & (df_course_question_subject.SubjectName == subject)]
    df_final1 = df_course_question_subject[(df_course_question_subject.COURSENAME == course)]
    df_final2 = df_course_question_subject[ (df_course_question_subject.SubjectName == subject)]
    df_final_hist = df_final[df_final._TotalQuestionAppear >= 20]
    fig = px.histogram(df_final_hist, x="_QuestionScore", nbins=150,title=f'QuestionScore histogram i.e. its distribution with subject- {subject} and course- {course}')
    fig.show()
    fig.write_image("images/question_accuracy_hist1.pdf")
    
    df_final1 = df_final1[df_final1._TotalQuestionAppear >= 20]
    fig = px.histogram(df_final1, x="_QuestionScore", nbins=150,title=f'QuestionScore histogram i.e. its distribution with  course- {course}')
    fig.show()
    fig.write_image("images/question_accuracy_hist2.pdf")

    df_final2 = df_final2[df_final2._TotalQuestionAppear >= 20]
    fig = px.histogram(df_final2, x="_QuestionScore", nbins=150,title=f'QuestionScore histogram i.e. its distribution with subject- {subject}')
    fig.show()
    fig.write_image("images/question_accuracy_hist3.pdf")

def question_heatmap():
    #total number of questions for each course with each subject
    df_temp = df_course_question_subject.groupby(['SubjectName', 'COURSENAME'], as_index=True).agg(questions_count=('QuestionId', 'count'))
    df_temp['questions_count'] = df_temp['questions_count']/100
    df_temp2 = df_temp.unstack(level='COURSENAME')
    df_temp2.columns = df_temp2.columns.droplevel()
    plt.rcParams['figure.figsize'] = [40, 20]
    sns.set(font_scale=2.5)
    fig, ax = plt.subplots(1, 2)
    sns.heatmap(df_temp2.iloc[:, :20], vmin=0, vmax=450, cmap = 'YlGnBu', linewidth=1, linecolor='w', annot = True, fmt = '.0f', annot_kws={'fontsize':22}, ax=ax[0])
    sns.heatmap(df_temp2.iloc[:, 20:], vmin=0, vmax=450, cmap = 'YlGnBu', linewidth=2, linecolor='w', annot = True, fmt = '.0f', annot_kws={'fontsize':22}, ax=ax[1])
    plt.suptitle("Distribution of number of questions for each course and corresponding subject (in 100)")
    plt.savefig("plots/images/question_number1.jpg")
    plt.show()
   


def check(str):
    temp1 = re.match("(6)*", str).group()
    temp2 = re.match("(7/8)*", str).group()
    temp3 = re.match("(9/10)*", str).group()
    temp4 = re.match("(11/12)*", str).group()
    return temp1 == '' and temp2 == '' and temp3 == '' and temp4 == ''
def question_heatmap2(courseType, questionType):
    QuestionType = questionType
    regex = f'({courseType})*'
    compiled = re.compile(regex)
    df_ = df_course_question_subject.copy()
    if courseType != "Others":
        df_['new'] = df_.COURSENAME.apply(lambda x: False if re.search(compiled, x).group() == '' else True)
    else: 
       df_['new'] = df_.COURSENAME.apply(lambda x: check(x))
    df_temp = df_[(df_.new == True) & (df_.QuestionType == QuestionType)]
    df_temp = df_temp.groupby(['SubjectName', 'COURSENAME'], as_index=True).agg(questions_count=('QuestionId', 'count'))

    df_temp['questions_count'] = df_temp['questions_count']/100
    df_temp = df_temp.unstack(level='COURSENAME')
    df_temp.columns = df_temp.columns.droplevel()
    plt.rcParams['figure.figsize'] = [40, 20]
    sns.set(font_scale=2.5)
    sns.heatmap(df_temp, vmin=0, vmax=450, cmap = 'YlGnBu', linewidth=2.5, linecolor='w', annot = True, fmt = '.0f', annot_kws={'fontsize':22})
    plt.suptitle(f"Distribution of number of {QuestionType} questions of course {courseType} and subject-wise (in 100)")
    plt.savefig("images/question_number2.pdf")
    plt.show()
df_video2 = pd.merge(df_video, df_course, left_on='courseid', right_on='COURSEID', how='left')
df_video3 = pd.merge(df_video2, df_subject_name, left_on='subjectid', right_on='SubjectId', how='left')
df_video3.drop(['COURSEID', 'SubjectId'], inplace=True, axis=1)
df_video3.columns = df_video3.columns.str.lower()

def videos():
    df_video_subject = df_video3.groupby(['subjectname'], as_index=False).agg(video_count=('subjectname', 'count'))
    fig = px.bar(df_video_subject, x='subjectname', y='video_count', title=f'No. of videos by subject',
             hover_data=['subjectname', 'video_count'], color='video_count', height=400)
    fig.show()
    df_video_course = df_video3.groupby(['coursename'], as_index=False).agg(video_count=('coursename', 'count'))
    fig = px.bar(df_video_course, x='coursename', y='video_count', title=f'No. of videos by course',
             hover_data=['coursename', 'video_count'], color='video_count', height=400)
    fig.show()
    fig.write_image("plots/images/videos1.jpg")

def videos2(course, subject):
    df_video_temp = df_video3[(df_video3.subjectname == subject)]
    df_video_temp = df_video_temp.groupby(['coursename'], as_index=False).agg(video_count=('coursename', 'count'))
    fig = px.bar(df_video_temp, x='coursename', y='video_count', title=f'No. of videos by course with subject- {subject}',
                hover_data=['coursename', 'video_count'], color='video_count', height=400)
    fig.show()
    df_video_temp = df_video3[(df_video3.coursename == course)]
    df_video_temp = df_video_temp.groupby(['subjectname'], as_index=False).agg(video_count=('subjectname', 'count'))
    fig = px.bar(df_video_temp, x='subjectname', y='video_count', title=f'No. of videos by subject with course- {course}',
             hover_data=['subjectname', 'video_count'], color='video_count', height=400)
    fig.show()
    fig.write_image("images/videos2.pdf")


def ksc_graph():
    df_course_ksc_temp = df_course_ksc.groupby(['courseid'], as_index=False).agg(ksc_count=('courseid', 'count'))
    df_course_ksc_temp = pd.merge(df_course_ksc_temp, df_course, left_on='courseid', right_on='COURSEID', how='left')
    data = df_course_ksc_temp
    fig = px.bar(df_course_ksc_temp, y='ksc_count', x='COURSENAME', text='ksc_count', title='ksc count with course')
    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig.update_layout(uniformtext_minsize=8)
    fig.show()
    fig.write_image("plots/images/ksc_count1.jpg")
    data = df_course_question_subject.groupby('SubjectName', as_index=False).agg(ksc_count=('SubjectName', 'count'))
    fig = px.bar(data, y='ksc_count', x='SubjectName', text='ksc_count', title='ksc count with subject')
    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig.update_layout(uniformtext_minsize=8)
    fig.show()
    fig.write_image("plots/images/ksc_count2.jpg")




############ function to choose period ################

def choose_period():
    """This function asks the user to choose a period
 
     Parameters:
     No Argument
 
     Returns:
     returns the variable to uniquely determine the period
 
    """
    vis = int(input('(For visualisation w.r.t. Month/Quarter enter 1, else enter 2)  '))
    mo = 0    # keeps track of whether month or quarter is choosen
    # year_range = input("Please enter the year range in format (2012-2015): ")  # comment-out to ask user to select year-range
    y1 = 2016 #int(year_range[:4])
    y2 = 2030 #int(year_range[5:])
    
    # widget to choose year
    y_tup = [(str(i), str(i)) for i in range(y1, y2+1)]
    choosen_y = widgets.Dropdown(
        options = y_tup,
        value = '2021',
        description = 'Year:',
    )
    
    # widget to choose month
    m_tup = [('January', '01'), ('February', '02'), ('March', '03'), ('April', '04'), ('May', '05'), ('June', '06'), 
                     ('July', '07'), ('August', '08'), ('September', '09'), ('October', '10'), ('November', '11'), ('December', '12')]
    choosen_m = widgets.Dropdown(
        options = m_tup,
        value = '01',
        description = 'Month:',
    )
    
    # widget to choose quarter
    q_tup = [('Q1', 'Q1'), ('Q2', 'Q2'), ('Q3', 'Q3'), ('Q4', 'Q4')]
    choosen_q = widgets.Dropdown(
        options = q_tup,
        value = 'Q1',
        description = 'Quarter:',
    )
    
    # widget to choose start date
    sd = widgets.DatePicker(
        description = 'Start Date: ',
        disabled = False
    )
    # widget to choose end date
    ed = widgets.DatePicker(
        description = 'End Date: ',
        disabled = False
    )
    
    # display only desired widgets to user
    if(vis == 1):
        display(choosen_y)
        mo = int(input("Enter 1 to choose months and 2 for Quarters: "))
        if(mo == 1):
            display(choosen_m)
        else:
            display(choosen_q)
    else:
        display(sd)
        display(ed)
    return vis, mo, choosen_y, choosen_m, choosen_q, sd, ed

############ function to choose start & end dates corresponding to a period ################

def choose_start_end_date(vis, mo, choosen_y, choosen_m, choosen_q, sd, ed):
    """This function determines the start & end dates corresponding to a period
 
     Parameters:
     argument1 (int): reference of visualisation, for Month/Quarter enter 1, else enter 2
     argument2 (int): Enter 1 for months and 2 for Quarters
     argument3, 4, 5, 6, 7 (widgets): year, month, quarter, start date, end date widgets respectively
 
     Returns:
     datetime, datetime: Returns the start date & end dates corresponding to the period
 
    """
    start_date = datetime.now()
    end_date = datetime.now()
    # determine start & end dates
    if(vis == 2):
        start_date = sd.value
        end_date = ed.value
    elif (mo == 1):
        start_date = pd.to_datetime(choosen_y.value + '-' + choosen_m.value) 
        end_date = pd.to_datetime(choosen_y.value + '-' + choosen_m.value) + pd.offsets.MonthEnd()
    else:
        start_date = pd.to_datetime(choosen_y.value + '-' + choosen_q.value) 
        end_date = pd.to_datetime(choosen_y.value + '-' + choosen_q.value) + pd.offsets.QuarterEnd()
        
    print(f"Choosen Start Date is: {start_date}")
    print(f"Choosen End Date is: {end_date}")   
    
    return start_date, end_date



def get_time_df(start_date, end_date):
    df_user_login2 = df_user_login[(df_user_login.createdon >= start_date) & (df_user_login.lastaccessedtime <= end_date)]
    df_user_login2['session_login_time'] = 0
    for i in range(df_user_login2.shape[0]):
        endon_val = df_user_login2.iloc[i, 2]
        created_on = df_user_login2.iloc[i, 1]
        last_time = df_user_login2.iloc[i, 4]
        
        if pd.isna(endon_val):
            df_user_login2.session_login_time[i] = (last_time - created_on).seconds
        else:
            df_user_login2.session_login_time[i] = (endon_val - created_on).seconds
            
    try:
        df_user_login2.drop(['createdon', 'endon', 'isactive', 'lastaccessedtime'], axis=1, inplace=True)
    except Exception as e:
        print(e)
        print("It is not present or already dropped")
    df_user_login2 = df_user_login2.groupby(['userid'], as_index=False).agg(session_login_time=('session_login_time', 'sum'),
                                                                        login_count=('userid', 'count'))
    df_batch.batchname = df_batch.batchname.apply(lambda x: x.strip())
    df_batch_user_login = pd.merge(df_batch_student, df_user_login2, on='userid', how='left')
    df_batch_user_login.dropna(inplace=True)
    df_batch_user_course_time = pd.merge(df_batch_user_login, df_batch, on='batchid', how='left')
    df_batch_user_course_time = pd.merge(df_batch_user_course_time, df_course, left_on='courseid', right_on='COURSEID', how='left')
    df_time_less_10 = df_batch_user_course_time[df_batch_user_course_time.session_login_time < 600]
    df_time_more_10 = df_batch_user_course_time[df_batch_user_course_time.session_login_time >= 600]
    
    return df_time_less_10, df_time_more_10

def plot_time(course, df, start_date, end_date):
    df_temp1 = df[df.COURSENAME == course]
    fig = px.histogram(df_temp1, x="login_count", nbins=20, title=f'login count for session time more that 10 min between {start_date} and {end_date} for course{course}')
    fig.show()
    data = df_temp1.session_login_time/60
    fig = px.histogram(data, x="session_login_time", title=f'session login time for more time more that 10 min  for course {course}')
    fig.show()
    fig.write_image("images/question_session_login_count.pdf")
    # return
    
    
df_temp = pd.read_sql('SELECT DISTINCT COURSEID FROM USERTESTSESSION', con=conn)
df_temp2 = pd.read_sql('SELECT DISTINCT SubjectId FROM USERTESTSESSION', con=conn)
df_temp = pd.merge(df_temp, df_course, on='COURSEID', how='left')
df_temp2 = pd.merge(df_temp2, df_subject_name, on='SubjectId', how='left')
df_temp2.dropna(inplace=True)

def question_accuracy_from_raw(course, subject):
    course_id = df_temp[df_temp.COURSENAME == course].values[0][0]
    subject_id = df_temp2[df_temp2.SubjectName == subject].values[0][0]
    
        
    sql_query = """SELECT  AttemptedOn, _UserId, UserTestSessionQuestion.UserTestSessionId, \
                QuestionId, IsCorrectlyAnswered, AnswerOption \
                FROM [speedlabs-anon].[dbo].[UserTestSessionQuestion] \
                RIGHT JOIN (SELECT UserTestSessionId, CourseId, SubjectId \
                FROM [speedlabs-anon].[dbo].[UserTestSession] \
                WHERE CourseId = ? AND SubjectId = ?) AS temp \
                ON temp.UserTestSessionId = UserTestSessionQuestion.UserTestSessionId"""

    df_user_test_session_question = pd.read_sql(sql_query, con = conn, params=[course_id, subject_id])
    #drop all nan and nat values
    df_user_test_session_question = df_user_test_session_question.dropna().reset_index(drop=True)


    #sort the dataframe in increasing order based on Attempedon
    df_user_test_session_question.sort_values(key=pd.to_datetime, by=['AttemptedOn'], ascending=True, axis=0, inplace=True)
        
    #drop duplicate rows with both _UserID and QuestionId same and keep only the first entry
    df_user_test_session_question = df_user_test_session_question.drop_duplicates(subset=['_UserId', 'QuestionId'], 
                                                                keep='first').reset_index(drop=True)

    #make all isCorrectlyAnswered values greater than 1 as 1
    df_user_test_session_question.IsCorrectlyAnswered = df_user_test_session_question.IsCorrectlyAnswered \
                                                        .apply(lambda x : 1 if x >= 1 else x) 
        

    #groupby  Question Id and for each questionId calculate TotalAttempts 
    #and Totally correctly answered to calculate QuestionAccuracy
    df_user_test_session_question_accuracy = df_user_test_session_question.groupby(['QuestionId'], as_index=False) \
                                                                        .agg(QuestionAccuracy=('IsCorrectlyAnswered', 'mean'), new=('QuestionId', 'count'))
        
    df_temp1 = df_user_test_session_question_accuracy[df_user_test_session_question_accuracy.new >= 20]
    fig = px.histogram(df_temp1, x="QuestionAccuracy",
                   title=f'Histogram of QuestionAccuracy with course {course} and {subject}',
                   opacity=0.8,
                   color_discrete_sequence=['indianred'] # color of histogram bars
                   )
    fig.show()
    fig.write_image("images/question_accuracy_raw.pdf")
        
        
        
        
def affiliation_engagement_trend(start_date, end_date):
    df_user = pd.read_sql('SELECT userid, affiliationcodeid ,lastloggedin FROM [speedlabs-anon].[dbo].[User]', con=conn)
    df_user_session = pd.read_sql('SELECT userid, createdon,endon,lastaccessedtime FROM userloginsession', con=conn)
    df_user_session.columns = df_user_session.columns.str.lower()
    df_user.dropna(inplace=True)
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    df_user.sort_values(by=['lastloggedin'], axis=0, inplace=True, ascending=True)
    df_user['lastloggedin'] = pd.to_datetime(df_user['lastloggedin'], infer_datetime_format=True)
    df_user['lastloggedin'] = df_user['lastloggedin'].dt.date
    df_user = df_user[(df_user.lastloggedin >= start_date) & (df_user.lastloggedin <= end_date)]
    df_temp = df_user.groupby(['lastloggedin'], as_index=False).agg(active_count=('lastloggedin', 'count'))
    fig = px.line(df_temp, x='lastloggedin', y='active_count', markers=True, title='daily trend of active students')
    fig.show()
    fig.write_image("plots/images/last_loggedin_daily_trend.jpg")
    
    df_temp.lastloggedin = pd.to_datetime(df_temp.lastloggedin)
    df_temp2 = df_temp
    df_temp2 = df_temp2.groupby(pd.Grouper(key='lastloggedin',freq='M')).agg({'active_count':'sum'}).reset_index()
    fig = px.line(df_temp2, x='lastloggedin', y='active_count', markers=True, title='It tells in each month how many students becomes inactive')
    fig.show()
    fig.write_image("plots/images/last_logged_in_monthly_trend.jpg")
    start_date = pd.to_datetime(start_date)
    df_temp2 = df_temp
    df_temp2 = df_temp2.groupby(pd.Grouper(key='lastloggedin',freq='W')).agg({'active_count':'sum'}).reset_index()
    fig = px.line(df_temp2, x='lastloggedin', y='active_count', markers=True, title='It tells In each week how many students becomes inactive')
    fig.show()
    fig.write_image("plots/images/last_logged_in_weekly_trend.jpg")
   
def session_plot(start_date, end_date):
    df_user = pd.read_sql('SELECT userid, affiliationcodeid ,lastloggedin FROM [speedlabs-anon].[dbo].[User]', con=conn)
    df_user_session = pd.read_sql('SELECT userid, createdon,endon,lastaccessedtime FROM userloginsession', con=conn)
    df_user_session.columns = df_user_session.columns.str.lower()
    df_user.dropna(inplace=True)
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    df_user.sort_values(by=['lastloggedin'], axis=0, inplace=True, ascending=True)
    df_user['lastloggedin'] = pd.to_datetime(df_user['lastloggedin'], infer_datetime_format=True)
    df_user['lastloggedin'] = df_user['lastloggedin'].dt.date
    df_user = df_user[(df_user.lastloggedin >= start_date) & (df_user.lastloggedin <= end_date)]
    df_user_session = pd.read_sql('SELECT userid, createdon,endon,lastaccessedtime FROM userloginsession', con=conn)
    df_user_session.columns = df_user_session.columns.str.lower()
    df_user_session_merge = pd.merge(df_user_session, df_user, on='userid', how='left')
    df_user_session_merge.createdon = pd.to_datetime(df_user_session_merge.createdon)
    df_user_session_merge.endon = pd.to_datetime(df_user_session_merge.endon)
    df_user_session_merge.lastaccessedtime = pd.to_datetime(df_user_session_merge.lastaccessedtime)

    df_user_session_merge['session_login_time'] = 0
    for i in range(df_user_session_merge.shape[0]):
        endon_val = df_user_session_merge.endon[i]
        created_on = df_user_session_merge.createdon[i]
        last_time = df_user_session_merge.lastaccessedtime[i]
        if pd.isna(endon_val):
            df_user_session_merge.session_login_time[i] = (last_time - created_on).seconds
        else:
            df_user_session_merge.session_login_time[i] = (endon_val - created_on).seconds
    df_temp = df_user_session_merge.copy()
    df_temp['lastaccesseddate'] = df_user_session_merge['lastaccessedtime'].dt.date
    df_temp = df_temp.groupby(['lastaccesseddate'], as_index=False).agg(active_count=('session_login_time', 'sum'))

    fig = px.line(df_temp, x='lastaccesseddate', y='active_count', markers=True, title='total session login time by date')
    fig.show()
    fig.write_image("plots/images/total_session_time.jpg")
    
    df_temp = df_user_session_merge.copy()
    df_temp['lastaccesseddate'] = df_user_session_merge['lastaccessedtime'].dt.date
    df_temp = df_temp.groupby(['lastaccesseddate'], as_index=False).agg(active_count=('session_login_time', 'mean'))

    fig = px.line(df_temp, x='lastaccesseddate', y='active_count', markers=True, title='Avg session login time by date')
    fig.show()
    fig.write_image("plots/images/average_session_time.jpg")
    