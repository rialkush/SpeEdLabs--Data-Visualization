# for creation of pdf
from fpdf import FPDF
# for time
from datetime import datetime

# contains functions to do processing for visualization
from companion import *
# contains functions to plot visulizations related to videos, question, ksc, affiliation-engagement & session
from funcs2 import *

# dimensions of page
WIDTH = 210
HEIGHT = 297

# creates the title of report
def create_title(pdf, start_date, end_date):
    pdf.set_font('Arial', '', 30)  
    pdf.ln(60)
    pdf.write(5, f"Analytics-Report")
    pdf.ln(10)
    pdf.set_font('Arial', '', 16)
    pdf.write(4, f'({start_date.strftime("%d/%m/%Y")} - {end_date.strftime("%d/%m/%Y")})')
    pdf.ln(5)
    
# function to execute all the functions related to Assignment Usage & save all the visualizations into "plots" folder 
# 'Off' argument is just to save the figure without giving a pop-up output
def exec_class_type0(x):
    x.assignment_usage_time_histogram_range_split('Off')
    x.assignment_usage_time_histogram_logscale('Off')
    x.cwsw_assignment_usage_heatmap('Off')
    x.cwsw_avg_assignment_usage_time_heatmap('Off')
    x.cwsw_questions_used_heatmap('Off')
    x.cwsw_unique_questions_used_heatmap('Off')
    x.cwsw_unique_users_heatmap('Off')
    x.cwsw_total_time_spent_heatmap('Off')
    x.cwsw_questions_per_assignment_usage_heatmap('Off')

# function to execute all the functions related to Practice Session Usage & save all the visualizations into "plots" folder       
def exec_class_type1(x):
    x.practice_session_time_histogram_range_split('Off')
    x.practice_session_time_histogram_logscale('Off')
    x.cwsw_practice_session_heatmap('Off')
    x.cwsw_avg_practice_session_time_heatmap('Off')
    x.cwsw_questions_used_heatmap('Off')
    x.cwsw_unique_questions_used_heatmap('Off')
    x.cwsw_unique_users_heatmap('Off')
    x.cwsw_total_time_spent_heatmap('Off')
    x.cwsw_questions_per_practice_session_heatmap('Off')

# function to execute all the functions related to Institute Test Usage, Self Test Usage & Learn Usage and save all the visualizations into "plots" folder   
def exec_class_type2(x, start_date, end_date):
    x.coursewise_analysis_1('Off')
    x.coursewise_analysis_2('Off')
    x.coursewise_analysis_3('Off')
    x.subjectwise_analysis_1('Off')
    x.subjectwise_analysis_2('Off')
    x.subjectwise_analysis_3('Off')
    x.affiliationwise_analysis('Off')
    # plots only when duration is more than a day
    if((end_date - start_date).days > 1):
        x.daily_trend_1('Off')
        x.daily_trend_2('Off')
        x.daily_trend_3('Off')
    # plots only when duration is more than a week
    if((end_date - start_date).days > 7):
        x.weekly_trend_1('Off')
        x.weekly_trend_2('Off')
        x.weekly_trend_3('Off')
    # plots only when duration is more than a month
    if((end_date - start_date).days > 31):
        x.monthly_trend_1('Off')
        x.monthly_trend_2('Off')
        x.monthly_trend_3('Off')
    # plots only when duration is more than a quarter
    if((end_date - start_date).days > 124):
        x.quarterly_trend_1('Off')
        x.quarterly_trend_2('Off')
        x.quarterly_trend_3('Off')
    # plots only when duration is more than a year
    if((end_date - start_date).days > 366):
        x.yearly_trend_1('Off')
        x.yearly_trend_2('Off')
        x.yearly_trend_3('Off')

# function to execute all the functions related to Question analytics & save all the visualizations into "plots" folder          
def exec_class_type3(x):
    x.cwsw_new_question_added('Off')
    x.cwsw_previous_year_question_count('Off')
    x.cwsw_question_count_by_accuracy('Off')

# function to execute all the functions related to Error Reports & save all the visualizations into "plots" folder        
def exec_class_type4(x):
    x.cwsw_reported_errors('Off')

# function to execute all the functions related to KSC Analytics & save all the visualizations into "plots" folder       
def exec_class_type5(x):
    x.cwsw_new_ksc_added('Off')

# insert the pages into report for Assignment Usage & Practice Session Usage    
def create_pdf_class_type1(pdf, class_name = 'Assignment_Usage'):
    pdf.image(f'./plots/{class_name}/1.jpg', 5, 20, WIDTH)
    pdf.image(f'./plots/{class_name}/2.jpg', 5, 110, WIDTH)
    pdf.image(f'./plots/{class_name}/3.jpg', 5, 200, WIDTH)
    
    pdf.add_page()
    pdf.image(f'./plots/{class_name}/4.jpg', 5, 20, WIDTH)
    pdf.image(f'./plots/{class_name}/5.jpg', 5, 110, WIDTH)
    pdf.image(f'./plots/{class_name}/6.jpg', 5, 200, WIDTH)
    
    pdf.add_page()
    pdf.image(f'./plots/{class_name}/7.jpg', 5, 20, WIDTH)
    pdf.image(f'./plots/{class_name}/8.jpg', 5, 110, WIDTH)
    pdf.image(f'./plots/{class_name}/9.jpg', 5, 200, WIDTH)

# insert the pages into report for Institute Test Usage, Self Test Usage & Learn Usage 
def create_pdf_class_type2(pdf, start_date, end_date, class_name = 'Institute_Test_Usage'):
    pdf.image(f'./plots/{class_name}/1.jpg', 5, 20, WIDTH)
    pdf.image(f'./plots/{class_name}/2.jpg', 5, 110, WIDTH)
    pdf.image(f'./plots/{class_name}/3.jpg', 5, 200, WIDTH)
    
    pdf.add_page()
    pdf.image(f'./plots/{class_name}/4.jpg', 5, 20, WIDTH)
    pdf.image(f'./plots/{class_name}/5.jpg', 5, 110, WIDTH)
    pdf.image(f'./plots/{class_name}/6.jpg', 5, 200, WIDTH)
    
    pdf.add_page()
    pdf.image(f'./plots/{class_name}/7.jpg', 5, 20, WIDTH)
    # plots only when duration is more than a day
    if((end_date - start_date).days > 1):
        pdf.image(f'./plots/{class_name}/8.jpg', 5, 110, WIDTH)
        pdf.image(f'./plots/{class_name}/9.jpg', 5, 200, WIDTH)
        
        pdf.add_page()
        pdf.image(f'./plots/{class_name}/10.jpg', 5, 20, WIDTH)
    # plots only when duration is more than a week    
    if((end_date - start_date).days > 7):
        pdf.image(f'./plots/{class_name}/11.jpg', 5, 110, WIDTH)
        pdf.image(f'./plots/{class_name}/12.jpg', 5, 200, WIDTH)
        
        pdf.add_page()
        pdf.image(f'./plots/{class_name}/13.jpg', 5, 20, WIDTH)
    # plots only when duration is more than a month    
    if((end_date - start_date).days > 31):
        pdf.image(f'./plots/{class_name}/14.jpg', 5, 110, WIDTH)
        pdf.image(f'./plots/{class_name}/15.jpg', 5, 200, WIDTH)
        
        pdf.add_page()
        pdf.image(f'./plots/{class_name}/16.jpg', 5, 20, WIDTH)
    # plots only when duration is more than a Quarter    
    if((end_date - start_date).days > 124):
        pdf.image(f'./plots/{class_name}/17.jpg', 5, 110, WIDTH)
        pdf.image(f'./plots/{class_name}/18.jpg', 5, 200, WIDTH)
        
        pdf.add_page()
        pdf.image(f'./plots/{class_name}/19.jpg', 5, 20, WIDTH)
    # plots only when duration is more than a year    
    if((end_date - start_date).days > 366):
        pdf.image(f'./plots/{class_name}/20.jpg', 5, 110, WIDTH)
        pdf.image(f'./plots/{class_name}/21.jpg', 5, 200, WIDTH)
        
        pdf.add_page()
        pdf.image(f'./plots/{class_name}/22.jpg', 5, 20, WIDTH) 

# insert the pages into report for Question_Analytics       
def create_pdf_class_type3(pdf, class_name = 'Question_Analytics'):
    pdf.image(f'./plots/{class_name}/1.jpg', 5, 20, WIDTH)
    pdf.image(f'./plots/{class_name}/2.jpg', 5, 110, WIDTH)
    pdf.image(f'./plots/{class_name}/3.jpg', 5, 200, WIDTH) 

# insert the pages into report for Error Reports   
def create_pdf_class_type4(pdf, class_name = 'Error_Reports'):
    pdf.image(f'./plots/{class_name}/1.jpg', 5, 20, WIDTH)

# insert the pages into report for KSC Analytics   
def create_pdf_class_type5(pdf, class_name = 'KSC_Analytics'):
    pdf.image(f'./plots/{class_name}/1.jpg', 5, 210, WIDTH)         

# function to create the report     
def create_report(conn, start_date, end_date, filename="report.pdf"):
    pdf = FPDF() # A4 (210 by 297 mm)

    # add title
    ''' Page-1 '''
    pdf.add_page()
    create_title(pdf, start_date, end_date)
    
    # add the plots related to Assignment Usage
    ''' Page - 2,3,4 '''
    pdf.add_page()
    pdf.set_font('Arial', '', 25)  
    pdf.text(5, 10, f"1. Assignment Usage")
    AU = Assignment_Usage(conn, start_date, end_date)
    if(len(AU.df_CSQE)):
        exec_class_type0(AU)
        create_pdf_class_type1(pdf, class_name = 'Assignment_Usage')
    else:
        pdf.set_font('Arial', '', 20)  
        pdf.text(5, 20, f"NO DATA FOUND !!!")
    print('Assignment Usage Done!!!')
    
    # add the plots related to Practice Session Usage
    ''' Page - 5,6,7 '''
    pdf.add_page()
    pdf.set_font('Arial', '', 25)  
    pdf.text(5, 10, f"2. Practice Session Usage")
    PS = Practice_Session_Usage(conn, start_date, end_date)
    if(len(PS.df_UserTest)):
        exec_class_type1(PS)
        create_pdf_class_type1(pdf, class_name = 'Practice_Session_Usage')
    else:
        pdf.set_font('Arial', '', 20)  
        pdf.text(5, 20, f"NO DATA FOUND !!!")
    print('Practice Session Usage Done!!!')
    
    # add the plots related to Institute Test Usage
    ''' Page - 8 to 16(max) '''
    pdf.add_page()
    pdf.set_font('Arial', '', 25)  
    pdf.text(5, 10, f"3. Institute Test Usage")
    IT = Institute_Test_Usage(conn, start_date, end_date)
    if(len(IT.df_CSQinsT)):
        exec_class_type2(IT, start_date, end_date)
        create_pdf_class_type2(pdf, start_date, end_date, class_name = 'Institute_Test_Usage')
    else:
        pdf.set_font('Arial', '', 20)  
        pdf.text(5, 20, f"NO DATA FOUND !!!")
    print('Institute Test Usage Done!!!')
    
    # add the plots related to Self Test Usage
    ''' Page - 16 to 24(max) '''
    pdf.add_page()
    pdf.set_font('Arial', '', 25)  
    pdf.text(5, 10, f"4. Self Test Usage")
    ST = Self_Test_Usage(conn, start_date, end_date)
    if(len(ST.df_UserTest)):
        exec_class_type2(ST, start_date, end_date)
        create_pdf_class_type2(pdf, start_date, end_date, class_name = 'Self_Test_Usage')
    else:
        pdf.set_font('Arial', '', 20)  
        pdf.text(5, 20, f"NO DATA FOUND !!!")
    print('Self Test Usage Done!!!')
    
    # add the plots related to Learn Usage
    ''' Page - 24 to 32(max) '''
    pdf.add_page()
    pdf.set_font('Arial', '', 25)  
    pdf.text(5, 10, f"5. Learn Usage")
    LU = Learn_Usage(conn, start_date, end_date)
    if(len(LU.df_LU)):
        exec_class_type2(LU, start_date, end_date)
        create_pdf_class_type2(pdf, start_date, end_date, class_name = 'Learn_Usage')
    else:
        pdf.set_font('Arial', '', 20)  
        pdf.text(5, 20, f"NO DATA FOUND !!!")
    print('Learn Usage Done!!!')
    
    # add the plots related to Question Analytics
    ''' Page - 33(max) '''
    pdf.add_page()
    pdf.set_font('Arial', '', 25)  
    pdf.text(5, 10, f"6. Question Analytics")
    QA = Question_Analytics(conn, start_date, end_date)
    if(len(QA.df_Q)):
        exec_class_type3(QA)
        create_pdf_class_type3(pdf, class_name = 'Question_Analytics')
    else:
        pdf.set_font('Arial', '', 20)  
        pdf.text(5, 20, f"NO DATA FOUND !!!")
    print('Question Analytics Done!!!')
    
    # add the plots related to Error Reports
    ''' Page - 34(max) '''
    pdf.add_page()
    pdf.set_font('Arial', '', 25)  
    pdf.text(5, 10, f"7. Error Reports")
    ER = Error_Reports(conn, start_date, end_date)
    if(len(ER.df_E)):
        exec_class_type4(ER)
        create_pdf_class_type4(pdf, class_name = 'Error_Reports')
    else:
        pdf.set_font('Arial', '', 20)  
        pdf.text(5, 20, f"NO DATA FOUND !!!")
    print('Error Reports Done!!!')
    
    # add the plots related to KSC Analytics
    ''' Page - 35(max) '''
    pdf.add_page()
    pdf.set_font('Arial', '', 25)  
    pdf.text(5, 10, f"8. KSC Analytics")
    KA = KSC_Analytics(conn, start_date, end_date)
    ksc_graph()
    pdf.image(f'./plots/images/ksc_count1.jpg', 30, 10, WIDTH/1.3)
    pdf.image(f'./plots/images/ksc_count2.jpg', 30, 100, WIDTH/1.3)
    if(len(KA.df_K)):
        exec_class_type5(KA)
        create_pdf_class_type5(pdf, class_name = 'KSC_Analytics')
    else:
        pdf.set_font('Arial', '', 20)  
        pdf.text(5, 250, f"NO DATA FOUND IN SPECIFIED PERIOD!!!")
    print('KSC Analytics Done!!!')
    
    # add the plots related to Question Heatmap & Video Analytics
    ''' Page - 36(max) '''
    pdf.add_page()
    pdf.set_font('Arial', '', 25)  
    pdf.text(5, 10, f"9. Question Heatmap")
    question_heatmap()
    pdf.image(f'./plots/images/question_number1.jpg', 5, 20, WIDTH)
    print('Question Heatmap Done!!!')
    pdf.set_font('Arial', '', 25)  
    pdf.text(5, 150, f"10. Video Analytics")
    videos()
    pdf.image(f'./plots/images/videos1.jpg', 5, 160, WIDTH)
    print('Video Analytics Done!!!')
    
    # add the plots related to Affiliation Engagement
    ''' Page - 37(max) '''
    pdf.add_page()
    pdf.set_font('Arial', '', 25)  
    pdf.text(5, 10, f"11. Affiliation Engagement")
    affiliation_engagement_trend(start_date, end_date)
    pdf.image(f'./plots/images/last_loggedin_daily_trend.jpg', 35, 20, WIDTH/1.5)
    pdf.image(f'./plots/images/last_logged_in_monthly_trend.jpg', 35, 110, WIDTH/1.5)
    pdf.image(f'./plots/images/last_logged_in_weekly_trend.jpg', 35, 200, WIDTH/1.5)
    print('Affiliation Engagement Done!!!')
    
    # add the plots related to Login Session
    ''' Page - 38(max) '''
    pdf.add_page()
    pdf.set_font('Arial', '', 25)  
    pdf.text(5, 10, f"12. Login Session")
    session_plot(start_date, end_date)
    pdf.image(f'./plots/images/total_session_time.jpg', 5, 15, WIDTH)
    pdf.image(f'./plots/images/average_session_time.jpg', 5, 160, WIDTH)
    print('Login Session Done!!!')
    
    print('ALL DONE!!!')
    
    # save the pdf with name = filename
    pdf.output(filename, 'F')
    
