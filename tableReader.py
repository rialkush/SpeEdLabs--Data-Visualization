import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pyodbc
import pymssql
from datetime import datetime
import ipywidgets as widgets
from IPython.display import display
from scipy import stats


########### function to read Question table #############

def read_question_table(conn, start_date, end_date):
    df_Q = pd.read_sql_query(f'''SELECT Question.QuestionId, Question._QuestionScore, 
      Question._SubjectId, Question.CreatedOn, Question.PastYearAppearance,
      CourseKSC.CourseId,
      Course.CourseName,
      Subject.SubjectName
      
      FROM Question
      
      JOIN CourseKSC ON Question.PrimaryKSCId = CourseKSC.KSCId
      
      JOIN Course ON CourseKSC.CourseId = Course.CourseId
      
      JOIN Subject ON Question._SubjectId = Subject.SubjectId ''', conn)
    return df_Q

########### function to read Error table #############

def read_error_table(conn, start_date, end_date):
    df_E = pd.read_sql_query(f'''SELECT RevisionQuestionErrorTag.QuestionId, RevisionQuestionErrorTag.CreatedOn,
      CourseKSC.CourseId,
      Course.CourseName,
      Subject.SubjectName
      
      FROM RevisionQuestionErrorTag
      
      JOIN Question ON RevisionQuestionErrorTag.QuestionId = Question.QuestionId
      
      JOIN CourseKSC ON Question.PrimaryKSCId = CourseKSC.KSCId
      
      JOIN Course On CourseKSC.CourseId = Course.CourseId
      
      JOIN Subject On Question._SubjectId = Subject.SubjectId
      
      WHERE CAST(RevisionQuestionErrorTag.CreatedOn  as date) BETWEEN '{start_date.strftime('%Y%m%d')}' AND '{end_date.strftime('%Y%m%d')}' ''', conn)
    return df_E

########### function to read KSC table #############

def read_ksc_table(conn, start_date, end_date):
    df_K = pd.read_sql_query(f'''SELECT KSC.KSCId, KSC.CreatedOn,
      CourseKSC.CourseId,
      Course.CourseName,
      Subject.SubjectName
      
      FROM KSC
      
      JOIN CourseKSC ON KSC.KSCId = CourseKSC.KSCId
      
      JOIN Course On CourseKSC.CourseId = Course.CourseId
      
      JOIN ChapterKSC ON KSC.KSCId = ChapterKSC.KSCId
      
      JOIN Chapter ON ChapterKSC.ChapterId = Chapter.ChapterId
      
      JOIN Topic ON Chapter.TopicId = Topic.TopicId
      
      JOIN Subject On Topic.SubjectId = Subject.SubjectId
      
      WHERE CAST(KSC.CreatedOn  as date) BETWEEN '{start_date.strftime('%Y%m%d')}' AND '{end_date.strftime('%Y%m%d')}' ''', conn)
    return df_K

########### function to read Exam table #############

def read_exam_table(conn, start_date, end_date):
    """This function reads the data related to Exam table
 
     Parameters:
     No Arguments
 
     Returns:
     Object: Dataframe containing all the info for visualisations related to Exam table
 
    """
    df_CSQE = pd.read_sql_query(f'''SELECT ExamSessionQuestion.ExamSessionQuestionId, ExamSessionQuestion.ExamSessionId,
      ExamSessionQuestion.QuestionId, ExamSessionQuestion.StartedOn, ExamSessionQuestion.CompletedOn, ExamSessionQuestion.TotalTimeTaken, 
      ExamSession.ExamId, ExamSession.UserId,
      ExamCourse.CourseId,
      [User].AffiliationCodeId,
      UserRole.RoleId,
      Question._SubjectId,
      Course.CourseName,
      Subject.SubjectName
      
      FROM ExamSessionQuestion
      
      JOIN ExamSession ON ExamSessionQuestion.ExamSessionId = ExamSession.ExamSessionId
      
      JOIN ExamCourse ON ExamSession.ExamId = ExamCourse.ExamId
      
      JOIN [User] ON ExamSession.UserId = [User].UserId
      
      JOIN UserRole ON ExamSession.UserId = UserRole.UserId
      
      JOIN Question ON ExamSessionQuestion.QuestionId = Question.QuestionId
      
      JOIN Course On ExamCourse.CourseId = Course.CourseId
      
      JOIN Subject On Question._SubjectId = Subject.SubjectId
      
      WHERE CAST(CompletedOn as date) BETWEEN '{start_date.strftime('%Y%m%d')}' and '{end_date.strftime('%Y%m%d')}' ''', conn)
    return df_CSQE

########### function to read UserTest table #############

def read_usertest_table(conn, start_date, end_date):
    """This function reads the data related to UserTest table
 
     Parameters:
     No Arguments
 
     Returns:
     Object: Dataframe containing all the info for visualisations related to UserTest table
 
    """
    df_UserTest = pd.read_sql_query(f'''SELECT UserTestSessionQuestion.UserTestSessionId, [User].UserId,
      UserTestSessionQuestion.QuestionId, UserTestSessionQuestion.IsAttempted, UserTestSessionQuestion.TimeTakenTillSubmission,
      UserTestSessionQuestion.StartedOn, UserTestSessionQuestion.CompletedOn, UserTestSessionQuestion.AttemptedOn, 
      UserTestSession.CourseId, UserTestSession.SubjectId,
      [User].AffiliationCodeId, [User].CenterCodeId,
      UserRole.RoleId,
      Course.CourseName,
      Subject.SubjectName
      
      FROM UserTestSessionQuestion
      
      JOIN UserTestSession ON UserTestSessionQuestion.UserTestSessionId = UserTestSession.UserTestSessionId
      
      JOIN [User] ON UserTestSessionQuestion._UserId = [User].UserId
      
      JOIN UserRole ON UserTestSessionQuestion._UserId = UserRole.UserId
      
      JOIN Course On UserTestSession.CourseId = Course.CourseId
      
      JOIN Subject On UserTestSession.SubjectId = Subject.SubjectId
      
      WHERE CAST(AttemptedOn as date) BETWEEN '{start_date.strftime('%Y%m%d')}' and '{end_date.strftime('%Y%m%d')}' ''', conn)
    return df_UserTest

########### function to read InstituteTest table #############

def read_instest_table(conn, start_date, end_date):
    """This function reads the data related to InstituteTest table
 
     Parameters:
     No Arguments
 
     Returns:
     Object: Dataframe containing all the info for visualisations related to InstituteTest table
 
    """
    df_CSQinsT = pd.read_sql_query(f'''SELECT InstituteTestUserQuestion.UserId, InstituteTestUserQuestion.InstituteTestId,
      InstituteTestUserQuestion.InstituteTestUserId, InstituteTestUserQuestion.QuestionId, InstituteTestUserQuestion.StartedOn, 
      InstituteTestUserQuestion.CompletedOn, InstituteTestUserQuestion.TimeTakenInSec, 
      InstituteTest.CourseId,
      [User].AffiliationCodeId, [User].CenterCodeId,
      UserRole.RoleId,
      Question._SubjectId,
      Course.CourseName,
      Subject.SubjectName
      
      FROM InstituteTestUserQuestion
      
      JOIN InstituteTest ON InstituteTestUserQuestion.InstituteTestId = InstituteTest.InstituteTestId
      
      JOIN [User] ON InstituteTestUserQuestion.UserId = [User].UserId
      
      JOIN UserRole ON InstituteTestUserQuestion.UserId = UserRole.UserId
      
      JOIN Question ON InstituteTestUserQuestion.QuestionId = Question.QuestionId
      
      JOIN Course ON InstituteTest.CourseId = Course.CourseId
      
      JOIN Subject ON Question._SubjectId = Subject.SubjectId
      
      WHERE CAST(StartedOn as date) BETWEEN '{start_date.strftime('%Y%m%d')}' and '{end_date.strftime('%Y%m%d')}' ''', conn)
    return df_CSQinsT


########### function to read OnlineClass table #############

def read_onlineclass_table(conn, start_date, end_date):
    """This function reads the data related to OnlineClass table
 
     Parameters:
     No Arguments
 
     Returns:
     Object: Dataframe containing all the info for visualisations related to OnlineClass table
 
    """
    df_LU = pd.read_sql_query(f'''SELECT OnlineClasses.OnlineClassId, OnlineClasses.AffiliationId, OnlineClasses.CourseId, 
      OnlineClasses.SubjectId, OnlineClasses.StartTime, OnlineClasses.Duration, 
      Course.CourseName,
      Subject.SubjectName
      
      FROM OnlineClasses
      
      JOIN Course On OnlineClasses.CourseId = Course.CourseId
      
      JOIN Subject On OnlineClasses.SubjectId = Subject.SubjectId
      
      WHERE CAST(StartTime as date) BETWEEN '{start_date.strftime('%Y%m%d')}' and '{end_date.strftime('%Y%m%d')}' ''', conn)
    return df_LU


