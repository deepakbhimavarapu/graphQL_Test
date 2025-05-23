from flask import Flask, request, jsonify
from ariadne import gql, load_schema_from_path, make_executable_schema, graphql_sync, QueryType
from ariadne.constants import PLAYGROUND_HTML
from google.cloud import bigquery
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import logging
from dateutil import parser
from dateutil.tz import UTC
from functools import lru_cache
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import threading
from ariadne import ObjectType
from ariadne.exceptions import HttpError
from graphql import GraphQLError
from ariadne.asgi import GraphQL
from ariadne.asgi.handlers import GraphQLHTTPHandler
from ariadne.types import Extension
import functools

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Initialize Flask
app = Flask(__name__)

def format_time_spent(seconds):
    """Convert seconds to HH:MM:SS format"""
    if not seconds:
        return "00:00:00"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# Load GraphQL schema
type_defs = gql(load_schema_from_path("schema.graphql"))
query = QueryType()

# Setup BigQuery Client
bq_client = bigquery.Client()

# Cache duration in seconds (5 minutes)
CACHE_DURATION = 300

# Cache for program data
program_cache = {
    'data': None,
    'timestamp': None
}

# Thread pool for CPU-bound tasks
thread_pool = ThreadPoolExecutor(max_workers=4)

def run_async(coro):
    """Run a coroutine in the current thread's event loop or create a new one if none exists"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

async def process_module(module, is_program_analytics=False):
    """Process a single module asynchronously"""
    module_dict = dict(module.items())
    
    if is_program_analytics:
        # Format for Program Performance Analytics (ProgramModule type)
        if module_dict.get('module_offering_start_timestamp'):
            module_dict['start_date'] = format_date(module_dict.pop('module_offering_start_timestamp'))
        if module_dict.get('module_offering_end_timestamp'):
            module_dict['end_date'] = format_date(module_dict.pop('module_offering_end_timestamp'))
        
        # Map fields to match ProgramModule type
        return {
            'module_id': module_dict.get('module_id'),
            'module_name': module_dict.get('module_name'),
            'cohort_schedule_id': module_dict.get('cohort_schedule_id'),
            'start_date': module_dict.get('start_date'),
            'end_date': module_dict.get('end_date'),
            'module_total_points': module_dict.get('module_total_points_defined', 0),
            'issue_badges': module_dict.get('module_issue_badges_flag', False),
            'badge_percentage': module_dict.get('module_badge_percentage', 0.0),
            'allowed_quiz_attempts': module_dict.get('module_allowed_quiz_attempts', 0),
            'sla_ita_attendance_points': module_dict.get('module_sla_ita_attendance_points', 0),
            'badge_bonus_points': module_dict.get('module_badge_bonus_points', 0)
        }
    else:
        # Format for Learner Progress (LearnerModule type)
        if module_dict.get('module_offering_start_timestamp'):
            module_dict['start_date'] = format_date(module_dict.pop('module_offering_start_timestamp'))
        
        return {
            'id': module_dict.get('module_id'),
            'name': module_dict.get('module_name'),
            'engagement': module_dict.get('engagement'),
            'progress': module_dict.get('progress'),
            'performance': module_dict.get('performance'),
            'badge': module_dict.get('badge'),
            'badge_bonus_points': module_dict.get('bonus_points'),
            'time_spent': format_time_spent(module_dict.get('time_spent_seconds_module', 0)),
            'achieved_points': module_dict.get('achieved_points'),
            'total_points': module_dict.get('module_total_points_defined'),
            'start_date': module_dict.get('start_date')
        }

async def process_course(course, is_program_analytics=False):
    """Process a single course and its modules asynchronously"""
    course_dict = dict(course.items())
    
    if is_program_analytics:
        # Format for Program Performance Analytics (ProgramCourse type)
        if course_dict.get('course_offering_start_timestamp'):
            course_dict['start_date'] = format_date(course_dict.pop('course_offering_start_timestamp'))
        if course_dict.get('course_offering_end_timestamp'):
            course_dict['end_date'] = format_date(course_dict.pop('course_offering_end_timestamp'))
        
        # Process modules in parallel
        modules = course_dict.get('modules', [])
        if modules:
            module_tasks = [process_module(module, is_program_analytics=True) for module in modules]
            processed_modules = await asyncio.gather(*module_tasks)
        else:
            processed_modules = []
        
        # Map fields to match ProgramCourse type using exact field names from SQL query
        return {
            'course_id': course_dict.get('course_id'),
            'course_name': course_dict.get('course_name'),
            'start_date': course_dict.get('start_date'),
            'end_date': course_dict.get('end_date'),
            'course_total_points': course_dict.get('course_total_points', 0),
            'issue_badges': course_dict.get('issue_badges', False),
            'badge_percentage': course_dict.get('badge_percentage', 0.0),
            'allowed_quiz_attempts': course_dict.get('allowed_quiz_attempts', 0),
            'badge_bonus_points': course_dict.get('badge_bonus_points', 0),
            'sla_ita_attendance_points': course_dict.get('sla_ita_attendance_points', 0),
            'cohort_schedule_id': course_dict.get('cohort_schedule_id'),
            'certificate_pass_percentage': course_dict.get('certificate_pass_percentage', 0.0),
            'modules': processed_modules
        }
    else:
        # Format for Learner Progress (LearnerCourse type)
        if course_dict.get('course_offering_start_timestamp'):
            course_dict['start_date'] = format_date(course_dict.pop('course_offering_start_timestamp'))
        
        # Process modules in parallel
        modules = course_dict.get('modules', [])
        if modules:
            module_tasks = [process_module(module, is_program_analytics=False) for module in modules]
            processed_modules = await asyncio.gather(*module_tasks)
        else:
            processed_modules = []
        
        # Map fields to match LearnerCourse type
        return {
            'id': course_dict.get('course_id'),
            'name': course_dict.get('course_name'),
            'progress': course_dict.get('progress'),
            'engagement': course_dict.get('engagement'),
            'performance': course_dict.get('performance'),
            'certificate_earned': course_dict.get('certificate_earned'),
            'time_spent': format_time_spent(course_dict.get('time_spent_seconds_course', 0)),
            'achieved_points': course_dict.get('achieved_points'),
            'total_points': course_dict.get('course_total_points_defined'),
            'total_badges': course_dict.get('total_badges'),
            'certificate_pass_percentage': course_dict.get('certificate_pass_percentage'),
            'start_date': course_dict.get('start_date'),
            'modules': processed_modules
        }

async def process_program(program_dict):
    """Process a single program, its courses, and modules asynchronously"""
    # Format last_updated timestamp
    last_updated = program_dict.get('last_updated')
    if last_updated:
        program_dict['last_updated'] = format_date(last_updated)
    
    # Calculate duration
    duration = calculate_duration(
        program_dict.get('offering_start_timestamp'),
        program_dict.get('offering_end_timestamp')
    )
    
    # Process courses in parallel
    courses = program_dict.get('courses', [])
    if courses:
        # Create tasks for all courses
        course_tasks = [process_course(course, is_program_analytics=True) for course in courses]
        # Wait for all course processing to complete
        processed_courses = await asyncio.gather(*course_tasks)
        program_dict['courses'] = processed_courses
    else:
        program_dict['courses'] = []
    
    # Remove timestamp fields and add duration
    program_dict.pop('offering_start_timestamp', None)
    program_dict.pop('offering_end_timestamp', None)
    program_dict['duration'] = duration
    
    return program_dict

async def execute_query(sql):
    """Execute BigQuery query asynchronously"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        thread_pool,
        lambda: bq_client.query(sql).result()
    )

async def process_all_programs(results):
    """Process all programs asynchronously"""
    # Create tasks for all programs
    program_tasks = [process_program(dict(row.items())) for row in results]
    # Wait for all program processing to complete
    return await asyncio.gather(*program_tasks)

def get_cached_programs():
    """Get cached program data if it's still valid"""
    current_time = time.time()
    if (program_cache['data'] is not None and 
        program_cache['timestamp'] is not None and 
        current_time - program_cache['timestamp'] < CACHE_DURATION):
        return program_cache['data']
    return None

def set_cached_programs(data):
    """Cache program data with current timestamp"""
    program_cache['data'] = data
    program_cache['timestamp'] = time.time()

def format_date(date_str):
    """Convert BigQuery timestamp to DD MMM, YYYY format"""
    if not date_str:
        return None
    try:
        # Parse the timestamp using dateutil parser which handles various formats
        date = parser.parse(date_str)
        formatted_date = date.strftime("%d %b, %Y")
        return formatted_date
    except (ValueError, TypeError) as e:
        logger.error(f"Error parsing date {date_str}: {str(e)}")
        return None

def format_duration(weeks, days):
    """Format duration string based on weeks and days"""
    if days == 0:
        return f"{weeks} weeks" if weeks != 1 else "1 week"
    elif weeks == 0:
        return f"{days} days" if days != 1 else "1 day"
    else:
        week_str = f"{weeks} weeks" if weeks != 1 else "1 week"
        day_str = f"{days} days" if days != 1 else "1 day"
        return f"{week_str} {day_str}"

def calculate_duration(start_date, end_date):
    """Calculate duration metrics"""
    if not start_date or not end_date:
        return None
        
    try:
        # Parse the timestamps using dateutil parser and ensure they're timezone-aware
        start = parser.parse(start_date)
        end = parser.parse(end_date)
        now = datetime.now(UTC)  # Make current time timezone-aware
        
        # Calculate total duration
        total_duration = end - start
        total_weeks = total_duration.days // 7
        total_days = total_duration.days % 7
        
        # Calculate elapsed time
        if now > end:
            elapsed = 100.0
            remaining_weeks = 0
            remaining_days = 0
        else:
            elapsed_duration = now - start
            total_duration_days = total_duration.days
            elapsed = (elapsed_duration.days / total_duration_days) * 100 if total_duration_days > 0 else 0
            elapsed = round(elapsed, 1)
            
            # Calculate remaining time
            remaining_duration = end - now
            remaining_weeks = remaining_duration.days // 7
            remaining_days = remaining_duration.days % 7
        
        # Format the remaining time and total duration
        remaining_str = format_duration(remaining_weeks, remaining_days)
        total_str = format_duration(total_weeks, total_days)
        
        result = {
            "start": format_date(start_date),
            "end": format_date(end_date),
            "time_elapsed": elapsed,
            "remaining_time": f"{remaining_str} of total {total_str}"
        }
        return result
    except (ValueError, TypeError) as e:
        logger.error(f"Error calculating duration: {str(e)}")
        return None

@query.field("programs")
def resolve_programs(_, info):
    # Check cache first
    cached_data = get_cached_programs()
    if cached_data is not None:
        return cached_data

    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset = os.getenv('BIGQUERY_DATASET')
    
    # Optimized query with materialized CTEs
    program_sql = f"""
        WITH ModuleData AS (
            SELECT 
                mo.programme_id_fk,
                mo.cohort_schedule_id,
                mo.course_id_fk,
                mo.module_id,
                m.module_name,
                CAST(mo.module_offering_start_timestamp AS STRING) AS module_offering_start_timestamp,
                CAST(mo.module_offering_end_timestamp AS STRING) AS module_offering_end_timestamp,
                mo.module_total_points_defined,
                mo.module_issue_badges_flag,
                mo.module_badge_percentage,
                mo.module_allowed_quiz_attempts,
                mo.module_sla_ita_attendance_points,
                mo.module_badge_bonus_points
            FROM
                `{project_id}.{dataset}.DimModuleOffering` mo
            LEFT JOIN
                `{project_id}.{dataset}.DimModule` m
            ON
                mo.module_id = m.module_id
        ),
        OngoingCourseProgress AS (
            SELECT DISTINCT
                dc.course_name,
                dco.course_order_id,
                dmo.module_order_id,
                dm.module_name,
                du.first_name,
                fmps.is_badge_earned_module,
                dco.programme_id_fk
            FROM
                `{project_id}.{dataset}.DimCourseOffering` AS dco
            INNER JOIN
                `{project_id}.{dataset}.DimCourse` AS dc
            ON
                dco.course_id = dc.course_id
            INNER JOIN
                `{project_id}.{dataset}.FactModulePerformanceSnapshot` AS fmps
            ON
                dco.cohort_schedule_id = fmps.cohort_identifier_fk
            INNER JOIN
                `{project_id}.{dataset}.DimModuleOffering` AS dmo
            ON
                dco.cohort_schedule_id = dmo.cohort_schedule_id
                AND fmps.module_id = dmo.module_id
            INNER JOIN
                `{project_id}.{dataset}.DimModule` AS dm
            ON
                fmps.module_id = dm.module_id
            INNER JOIN
                `{project_id}.{dataset}.DimUser` AS du
            ON
                fmps.user_id = du.user_id
            WHERE
                dco.course_offering_end_timestamp > CURRENT_TIMESTAMP()
                AND dco.course_offering_start_timestamp <= CURRENT_TIMESTAMP()
        ),
        CourseData AS (
            SELECT 
                co.programme_id_fk,
                co.cohort_identifier_fk,
                co.course_id,
                c.course_name,
                CAST(co.course_offering_start_timestamp AS STRING) AS course_offering_start_timestamp,
                CAST(co.course_offering_end_timestamp AS STRING) AS course_offering_end_timestamp,
                co.course_total_points_defined,
                co.course_issue_badges_flag,
                co.course_badge_percentage,
                co.course_allowed_quiz_attempts,
                co.course_badge_bonus_points,
                co.course_sla_ita_attendance_points,
                co.cohort_schedule_id,
                COALESCE(co.certificate_pass_percentage, 0.0) as certificate_pass_percentage,
                ARRAY(
                    SELECT AS STRUCT
                        md.*
                    FROM
                        ModuleData md
                    WHERE
                        md.programme_id_fk = co.programme_id_fk
                        AND md.cohort_schedule_id = co.cohort_schedule_id
                        AND md.course_id_fk = co.course_id
                ) AS modules
            FROM
                `{project_id}.{dataset}.DimCourseOffering` co
            LEFT JOIN
                `{project_id}.{dataset}.DimCourse` c
            ON
                co.course_id = c.course_id
            WHERE
                co.programme_id_fk IS NOT NULL
        ),
        InactiveUsers AS (
            SELECT
                f.programme_id,
                ARRAY_AGG(DISTINCT u.first_name) as inactive_user_names
            FROM
                `{project_id}.{dataset}.FactProgrammePerformanceSnapshot` f
            JOIN
                `{project_id}.{dataset}.DimUser` u ON f.user_id = u.user_id
            WHERE
                f.is_alive = false
            GROUP BY
                f.programme_id
        ),
        BlockedUsers AS (
            SELECT
                programme_id,
                ARRAY_AGG(DISTINCT u.first_name) as blocked_user_names
            FROM
                `{project_id}.{dataset}.fact_user_programme_enrolment` e
            JOIN
                `{project_id}.{dataset}.DimUser` u ON e.user_id = u.user_id
            WHERE
                e.is_access_blocked_enrolment = true
            GROUP BY
                programme_id
        ),
        LaggingCourses AS (
            SELECT
                programme_id_fk as programme_id,
                ARRAY_AGG(
                    STRUCT(
                        first_name as user,
                        course_records_count as count
                    )
                ) as lagging_users
            FROM (
                SELECT
                    t3.programme_id_fk,
                    t4.first_name,
                    COUNT(DISTINCT CONCAT(t1.cohort_schedule_id, '-', t2.course_name)) AS course_records_count
                FROM
                    `{project_id}.{dataset}.DimCourseOffering` AS t1
                INNER JOIN
                    `{project_id}.{dataset}.FactCoursePerformanceSnapshot` AS t3
                ON
                    t1.cohort_schedule_id = t3.cohort_identifier_fk
                INNER JOIN
                    `{project_id}.{dataset}.DimCourse` AS t2
                ON
                    t1.course_id = t2.course_id
                INNER JOIN
                    `{project_id}.{dataset}.DimUser` AS t4
                ON
                    t3.user_id = t4.user_id
                WHERE
                    t1.course_offering_end_timestamp < CURRENT_TIMESTAMP()
                    AND t3.is_certificate_earned_course = FALSE
                GROUP BY
                    t3.programme_id_fk,
                    t4.first_name
            )
            GROUP BY
                programme_id_fk
        ),
        CompletedCourses AS (
            SELECT
                programme_id_fk as programme_id,
                ARRAY_AGG(
                    STRUCT(
                        t0.course_id as course_id,
                        DimCourse.course_name as course_name
                    )
                ) as completed_courses
            FROM
                `{project_id}.{dataset}.DimCourseOffering` AS t0
            INNER JOIN
                `{project_id}.{dataset}.DimCourse` AS DimCourse
            ON
                t0.course_id = DimCourse.course_id
            GROUP BY
                programme_id_fk
        ),
        ProgramMetrics AS (
            SELECT
                programme_id,
                ROUND(AVG(progress_percentage), 2) as avg_progress,
                ROUND(AVG(engagement_score_percentage), 2) as avg_engagement
            FROM
                `{project_id}.{dataset}.FactProgrammePerformanceSnapshot`
            GROUP BY
                programme_id
        ),
        LearnerData AS (
            SELECT
                programme_id,
                COUNT(DISTINCT user_id) as total_learners,
                COUNT(DISTINCT user_id) as active_learners,
                COUNT(DISTINCT CASE WHEN learner_category = 'Ahead' THEN user_id END) as ahead_count,
                COUNT(DISTINCT CASE WHEN learner_category = 'On Track' THEN user_id END) as on_track_count,
                COUNT(DISTINCT CASE WHEN learner_category = 'Behind' THEN user_id END) as behind_count
            FROM
                `{project_id}.{dataset}.FactProgrammePerformanceSnapshot`
            GROUP BY
                programme_id
        ),
        ProgrammeUserCertStatus AS (
            SELECT
                t1.course_order_id,
                DimCourse.course_name,
                DimUser.first_name,
                t0.is_certificate_earned_course,
                t0.programme_id_fk
            FROM
                `{project_id}.{dataset}.FactCoursePerformanceSnapshot` AS t0
            INNER JOIN
                `{project_id}.{dataset}.DimCourse` AS DimCourse
            ON
                t0.course_id = DimCourse.course_id
            INNER JOIN
                `{project_id}.{dataset}.DimUser` AS DimUser
            ON
                t0.user_id = DimUser.user_id
            INNER JOIN
                `{project_id}.{dataset}.DimProgramme` AS DimProgramme
            ON
                t0.programme_id_fk = DimProgramme.programme_id
            INNER JOIN
                `{project_id}.{dataset}.DimCourseOffering` AS t1
            ON
                t0.programme_id_fk = t1.programme_id_fk
                AND t0.course_id = t1.course_id
        )
        SELECT
            p.programme_id AS id,
            p.programme_name AS name,
            o.pm_email AS pm_email,
            o.pm_name AS program_manager,
            o.offering_status AS status,
            CAST(o.updated_on_timestamp AS STRING) AS last_updated,
            o.available_platform AS available_platform,
            o.defined_total_courses AS total_course_certificates,
            CAST(o.offering_start_timestamp AS STRING) AS offering_start_timestamp,
            CAST(o.offering_end_timestamp AS STRING) AS offering_end_timestamp,
            o.category AS category,
            o.cohort_identifier AS cohort,
            o.program_total_points_defined AS total_points,
            o.certificate_pass_percentage AS certificate_pass_percentage,
            o.programme_badge_percentage AS badge_percentage,
            o.distinction_percentage AS distinction_percentage,
            o.programme_allowed_quiz_attempts AS allowed_quiz_attempts,
            o.programme_badge_bonus_points AS badge_bonus_points,
            CAST(o.is_valid_offering AS BOOL) AS is_valid,
            ARRAY_AGG(
                STRUCT(
                    cd.course_id,
                    cd.course_name,
                    cd.course_offering_start_timestamp,
                    cd.course_offering_end_timestamp,
                    cd.course_total_points_defined AS course_total_points,
                    cd.course_issue_badges_flag AS issue_badges,
                    cd.course_badge_percentage AS badge_percentage,
                    cd.course_allowed_quiz_attempts AS allowed_quiz_attempts,
                    cd.course_badge_bonus_points AS badge_bonus_points,
                    cd.course_sla_ita_attendance_points AS sla_ita_attendance_points,
                    cd.cohort_schedule_id,
                    cd.certificate_pass_percentage,
                    cd.modules
                )
            ) AS courses,
            STRUCT(
                COALESCE(ld.total_learners, 0) as total,
                COALESCE(ld.active_learners, 0) as active,
                STRUCT(
                    COALESCE(ld.ahead_count, 0) as ahead,
                    COALESCE(ld.on_track_count, 0) as on_track,
                    COALESCE(ld.behind_count, 0) as behind
                ) as pacing
            ) as learners,
            COALESCE(ARRAY_LENGTH(iu.inactive_user_names), 0) as is_alive,
            COALESCE(iu.inactive_user_names, []) as is_alive_list,
            COALESCE(ARRAY_LENGTH(bu.blocked_user_names), 0) as access_blocked,
            COALESCE(bu.blocked_user_names, []) as access_blocked_list,
            COALESCE(ARRAY_LENGTH(lc.lagging_users), 0) as laggards_count,
            COALESCE(lc.lagging_users, []) as lagging_courses_list,
            COALESCE(ARRAY_LENGTH(cc.completed_courses), 0) as completed_course_count,
            COALESCE(cc.completed_courses, []) as completed_courses_list,
            COALESCE(pm.avg_progress, 0.0) as progress,
            COALESCE(pm.avg_engagement, 0.0) as engagement,
            ARRAY(
                SELECT AS STRUCT
                    ocp.course_name,
                    ocp.course_order_id,
                    ocp.module_order_id,
                    ocp.module_name,
                    ocp.first_name,
                    ocp.is_badge_earned_module
                FROM OngoingCourseProgress ocp
                WHERE ocp.programme_id_fk = p.programme_id
            ) as ongoing_course_progress,
            ARRAY(
                SELECT AS STRUCT
                    pucs.course_order_id,
                    pucs.course_name,
                    pucs.first_name,
                    pucs.is_certificate_earned_course
                FROM ProgrammeUserCertStatus pucs
                WHERE pucs.programme_id_fk = p.programme_id
            ) as programme_user_cert_status_list
        FROM
            `{project_id}.{dataset}.DimProgramme` p
        LEFT JOIN
            `{project_id}.{dataset}.DimProgrammeOffering` o
        ON
            p.programme_id = o.programme_id
        LEFT JOIN
            CourseData cd
        ON
            p.programme_id = cd.programme_id_fk
            AND o.cohort_identifier = cd.cohort_identifier_fk
        LEFT JOIN
            LearnerData ld
        ON
            p.programme_id = ld.programme_id
        LEFT JOIN
            InactiveUsers iu
        ON
            p.programme_id = iu.programme_id
        LEFT JOIN
            BlockedUsers bu
        ON
            p.programme_id = bu.programme_id
        LEFT JOIN
            LaggingCourses lc
        ON
            p.programme_id = lc.programme_id
        LEFT JOIN
            CompletedCourses cc
        ON
            p.programme_id = cc.programme_id
        LEFT JOIN
            ProgramMetrics pm
        ON
            p.programme_id = pm.programme_id
        WHERE
            CAST(o.is_valid_offering AS INT64) = CAST(1 AS INT64)
        GROUP BY
            id, name, pm_email, program_manager, status, last_updated,
            available_platform, total_course_certificates, offering_start_timestamp,
            offering_end_timestamp, category, cohort, total_points,
            certificate_pass_percentage, badge_percentage, distinction_percentage,
            allowed_quiz_attempts, badge_bonus_points, is_valid,
            ld.total_learners, ld.active_learners,
            ld.ahead_count, ld.on_track_count, ld.behind_count,
            iu.inactive_user_names,
            bu.blocked_user_names,
            lc.lagging_users,
            cc.completed_courses,
            pm.avg_progress,
            pm.avg_engagement
    """
    
    start_time = time.time()
    
    try:
        # Execute query and process results using run_async
        async def execute_and_process():
            results = await execute_query(program_sql)
            return await process_all_programs(results)
        
        programs = run_async(execute_and_process())
        
        # Cache the processed data
        set_cached_programs(programs)
        
        return programs
    except Exception as e:
        logger.error(f"Error in resolve_programs: {str(e)}")
        raise GraphQLError(f"Error fetching program data: {str(e)}")

def async_to_sync(func):
    """Convert an async function to sync function."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(func(*args, **kwargs))
        except RuntimeError as e:
            if "while another loop is running" in str(e):
                # If we can't use the current loop, create a new one
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(func(*args, **kwargs))
                finally:
                    new_loop.close()
            raise
    return wrapper

async def resolve_learner_progress(_, info, programme_id: str) -> dict:
    """
    Resolver for fetching learner progress data for a specific programme.
    """
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset = os.getenv('BIGQUERY_DATASET')
    
    query = f"""
    WITH ProgrammeInfo AS (
        SELECT 
            p.programme_id,
            p.programme_name,
            po.offering_status,
            po.defined_total_course_certificates,
            (
                SELECT COALESCE(SUM(total_badges), 0)
                FROM `{project_id}.{dataset}.DimCourseOffering` dco
                WHERE dco.programme_id_fk = p.programme_id
            ) as total_badges
        FROM `{project_id}.{dataset}.DimProgramme` p
        LEFT JOIN `{project_id}.{dataset}.DimProgrammeOffering` po
            ON p.programme_id = po.programme_id
        WHERE p.programme_id = @programme_id
    ),
    PerformanceMetrics AS (
        SELECT 
            programme_id,
            COUNT(CASE WHEN completion_status = true THEN 1 END) as completion_certificates,
            COUNT(CASE WHEN distinction_status = true THEN 1 END) as distinction_certificates,
            ROUND(AVG(COALESCE(performance_score_percentage, 0)), 2) as performance
        FROM `{project_id}.{dataset}.FactProgrammePerformanceSnapshot`
        WHERE programme_id = @programme_id
        GROUP BY programme_id
    ),
    ModuleData AS (
        SELECT 
            f.user_id,
            f.course_id_fk,
            f.module_id,
            m.module_name,
            CAST(mo.module_offering_start_timestamp AS STRING) AS module_offering_start_timestamp,
            CAST(mo.module_offering_end_timestamp AS STRING) AS module_offering_end_timestamp,
            mo.module_total_points_defined,
            mo.module_issue_badges_flag,
            mo.module_badge_percentage,
            mo.module_allowed_quiz_attempts,
            mo.module_sla_ita_attendance_points,
            mo.module_badge_bonus_points,
            ROUND(COALESCE(f.engagement_score_module_percentage, 0), 2) as engagement,
            ROUND(COALESCE(f.progress_percentage_module, 0), 2) as progress,
            ROUND(COALESCE(f.performance_score_module_percentage, 0), 2) as performance,
            COALESCE(f.is_badge_earned_module, false) as badge,
            COALESCE(f.earned_bonus_points_module, 0) as bonus_points,
            COALESCE(f.time_spent_seconds_module, 0) as time_spent_seconds_module,
            COALESCE(f.achieved_points_module, 0) as achieved_points
        FROM `{project_id}.{dataset}.FactModulePerformanceSnapshot` f
        JOIN `{project_id}.{dataset}.DimModule` m
            ON f.module_id = m.module_id
        JOIN `{project_id}.{dataset}.DimModuleOffering` mo
            ON f.programme_id_fk = mo.programme_id_fk
            AND f.module_id = mo.module_id
            AND f.cohort_identifier_fk = mo.cohort_schedule_id
        WHERE f.programme_id_fk = @programme_id
    ),
    LearnerCourses AS (
        SELECT 
            f.user_id,
            f.course_id,
            c.course_name,
            CAST(co.course_offering_start_timestamp AS STRING) AS course_offering_start_timestamp,
            CAST(co.course_offering_end_timestamp AS STRING) AS course_offering_end_timestamp,
            co.course_total_points_defined,
            co.course_issue_badges_flag,
            co.course_badge_percentage,
            co.course_allowed_quiz_attempts,
            co.course_badge_bonus_points,
            co.course_sla_ita_attendance_points,
            co.cohort_schedule_id,
            co.certificate_pass_percentage,
            co.total_badges,
            ROUND(COALESCE(f.progress_percentage_course, 0), 2) as progress,
            ROUND(COALESCE(f.engagement_score_course_percentage, 0), 2) as engagement,
            ROUND(COALESCE(f.performance_score_course_percentage, 0), 2) as performance,
            COALESCE(f.is_certificate_earned_course, false) as certificate_earned,
            COALESCE(f.time_spent_seconds_course, 0) as time_spent_seconds_course,
            COALESCE(f.achieved_points_course, 0) as achieved_points,
            ARRAY(
                SELECT AS STRUCT
                    md.module_id,
                    md.module_name,
                    md.module_offering_start_timestamp,
                    md.module_offering_end_timestamp,
                    md.module_total_points_defined,
                    md.module_issue_badges_flag,
                    md.module_badge_percentage,
                    md.module_allowed_quiz_attempts,
                    md.module_sla_ita_attendance_points,
                    md.module_badge_bonus_points,
                    md.engagement,
                    md.progress,
                    md.performance,
                    md.badge,
                    md.bonus_points,
                    md.time_spent_seconds_module,
                    md.achieved_points
                FROM ModuleData md
                WHERE md.user_id = f.user_id
                AND md.course_id_fk = f.course_id
            ) as modules
        FROM `{project_id}.{dataset}.FactCoursePerformanceSnapshot` f
        JOIN `{project_id}.{dataset}.DimCourse` c
            ON f.course_id = c.course_id
        JOIN `{project_id}.{dataset}.DimCourseOffering` co
            ON f.programme_id_fk = co.programme_id_fk
            AND f.course_id = co.course_id
            AND f.cohort_identifier_fk = co.cohort_schedule_id
        WHERE f.programme_id_fk = @programme_id
    ),
    LearnerData AS (
        SELECT 
            f.user_id,
            u.first_name as name,
            u.email,
            f.learner_category as status,
            ROUND(COALESCE(f.progress_percentage, 0), 2) as progress,
            ROUND(COALESCE(f.engagement_score_percentage, 0), 2) as engagement,
            ROUND(COALESCE(f.performance_score_percentage, 0), 2) as performance,
            COALESCE(f.time_spent_seconds, 0) as time_spent_seconds,
            COALESCE(f.achieved_points, 0) as achieved_points,
            COALESCE(f.badges_earned_count, 0) as badges,
            COALESCE(f.total_unlocked_course_points_in_programme, 0) as total_points,
            CASE WHEN f.completion_status = true THEN 1 ELSE 0 END as completion_certificate,
            CASE WHEN f.distinction_status = true THEN 1 ELSE 0 END as distinction_certificate,
            (
                SELECT COUNT(*)
                FROM `{project_id}.{dataset}.FactCoursePerformanceSnapshot` fcps
                WHERE fcps.programme_id_fk = @programme_id
                AND fcps.user_id = f.user_id
                AND fcps.is_certificate_earned_course = true
            ) as course_certificates_earned,
            CAST(f.last_active AS STRING) as last_active,
            ARRAY(
                SELECT AS STRUCT
                    lc.course_id,
                    lc.course_name,
                    lc.course_offering_start_timestamp,
                    lc.course_offering_end_timestamp,
                    lc.course_total_points_defined,
                    lc.course_issue_badges_flag,
                    lc.course_badge_percentage,
                    lc.course_allowed_quiz_attempts,
                    lc.course_badge_bonus_points,
                    lc.course_sla_ita_attendance_points,
                    lc.cohort_schedule_id,
                    lc.certificate_pass_percentage,
                    lc.total_badges,
                    lc.progress,
                    lc.engagement,
                    lc.performance,
                    lc.certificate_earned,
                    lc.time_spent_seconds_course,
                    lc.achieved_points,
                    lc.modules
                FROM LearnerCourses lc
                WHERE lc.user_id = f.user_id
            ) as courses
        FROM `{project_id}.{dataset}.FactProgrammePerformanceSnapshot` f
        JOIN `{project_id}.{dataset}.DimUser` u
            ON f.user_id = u.user_id
        WHERE f.programme_id = @programme_id
    )
    SELECT 
        pi.programme_id as id,
        pi.programme_name as name,
        pi.offering_status as status,
        COALESCE(pm.completion_certificates, 0) as completion_certificates,
        COALESCE(pm.distinction_certificates, 0) as distinction_certificates,
        COALESCE(pm.performance, 0.0) as performance,
        COALESCE(pi.defined_total_course_certificates, 0) as total_course_certificates,
        COALESCE(pi.total_badges, 0) as total_badges,
        ARRAY(
            SELECT AS STRUCT
                ld.user_id as id,
                ld.name,
                ld.email,
                ld.status,
                ld.progress,
                ld.engagement,
                ld.performance,
                ld.time_spent_seconds,
                ld.achieved_points,
                ld.badges,
                ld.total_points,
                STRUCT(
                    ld.completion_certificate as completion,
                    ld.distinction_certificate as distinction
                ) as certificates,
                ld.course_certificates_earned,
                ld.last_active,
                ld.courses
            FROM LearnerData ld
        ) as learner_data
    FROM ProgrammeInfo pi
    LEFT JOIN PerformanceMetrics pm
        ON pi.programme_id = pm.programme_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("programme_id", "STRING", programme_id)
        ]
    )

    try:
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        
        row = next(iter(results), None)
        if row:
            row_dict = dict(row.items())
            return {
                "id": row_dict["id"],
                "name": row_dict["name"],
                "status": row_dict["status"],
                "completion_certificates": row_dict["completion_certificates"],
                "distinction_certificates": row_dict["distinction_certificates"],
                "performance": row_dict["performance"],
                "total_course_certificates": row_dict["total_course_certificates"],
                "total_badges": row_dict["total_badges"],
                "learner_data": [
                    {
                        "id": learner["id"],
                        "name": learner["name"],
                        "email": learner["email"],
                        "status": learner["status"],
                        "progress": learner["progress"],
                        "engagement": learner["engagement"],
                        "performance": learner["performance"],
                        "time_spent": format_time_spent(learner["time_spent_seconds"]),
                        "achieved_points": learner["achieved_points"],
                        "badges": learner["badges"],
                        "total_points": learner["total_points"],
                        "certificates": learner["certificates"],
                        "course_certificates_earned": learner["course_certificates_earned"],
                        "last_active": format_date(learner["last_active"]),
                        "courses": [
                            {
                                "id": course["course_id"],
                                "name": course["course_name"],
                                "progress": course["progress"],
                                "engagement": course["engagement"],
                                "performance": course["performance"],
                                "certificate_earned": course["certificate_earned"],
                                "time_spent": format_time_spent(course["time_spent_seconds_course"]),
                                "achieved_points": course["achieved_points"],
                                "total_points": course["course_total_points_defined"],
                                "total_badges": course["total_badges"],
                                "certificate_pass_percentage": course["certificate_pass_percentage"],
                                "start_date": format_date(course["course_offering_start_timestamp"]),
                                "modules": [
                                    {
                                        "id": module["module_id"],
                                        "name": module["module_name"],
                                        "engagement": module["engagement"],
                                        "progress": module["progress"],
                                        "performance": module["performance"],
                                        "badge": module["badge"],
                                        "bonus_points": module["bonus_points"],
                                        "time_spent": format_time_spent(module["time_spent_seconds_module"]),
                                        "achieved_points": module["achieved_points"],
                                        "total_points": module["module_total_points_defined"],
                                        "start_date": format_date(module["module_offering_start_timestamp"])
                                    }
                                    for module in course["modules"]
                                ] if course["modules"] else []
                            }
                            for course in learner["courses"]
                        ] if learner["courses"] else []
                    }
                    for learner in row_dict["learner_data"]
                ] if row_dict["learner_data"] else []
            }
        return None

    except Exception as e:
        logger.error(f"Error in resolve_learner_progress: {str(e)}")
        raise GraphQLError(f"Error fetching learner progress data: {str(e)}")

# Create a sync version of the async resolver
sync_resolve_learner_progress = async_to_sync(resolve_learner_progress)

# Bind resolvers to query type
query.set_field("programs", resolve_programs)
query.set_field("learnerProgress", sync_resolve_learner_progress)

# Make the schema executable
schema = make_executable_schema(type_defs, query)

# Routes
@app.route("/graphql", methods=["GET"])
def graphql_playground():
    return PLAYGROUND_HTML, 200

@app.route("/graphql", methods=["POST"])
def graphql_server():
    data = request.get_json()
    success, result = graphql_sync(
        schema,
        data,
        context_value=request,
        debug=True
    )
    status_code = 200 if success else 400
    return jsonify(result), status_code

if __name__ == "__main__":
    app.run(debug=True)
