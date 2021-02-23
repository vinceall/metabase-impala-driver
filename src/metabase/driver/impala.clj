(ns metabase.driver.impala
  "Impala driver. Builds off of the SQL-JDBC driver."
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [honeysql.core :as hsql]
            [java-time :as t]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.db.spec :as dbspec]
            [metabase.driver.common :as driver.common]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.query-processor.timezone :as qp.timezone]
            [metabase.util
             [honeysql-extensions :as hx]
             [i18n :refer [trs]]
             [ssh :as ssh]])
  (:import [java.sql DatabaseMetaData ResultSet ResultSetMetaData Types Timestamp]
           [java.time OffsetDateTime OffsetTime ZonedDateTime]))

(driver/register! :impala, :parent :sql-jdbc)

(defmethod driver/display-name :impala [_] "Impala")


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- db-version [^DatabaseMetaData metadata]
  (Double/parseDouble
    (format "%d.%d" (.getDatabaseMajorVersion metadata) (.getDatabaseMinorVersion metadata))))

(defn- warn-on-unsupported-versions [driver details]
  (let [jdbc-spec (sql-jdbc.conn/details->connection-spec-for-testing-connection driver details)]
    (jdbc/with-db-metadata [metadata jdbc-spec]
                           (when (< (db-version metadata) 5.7)
                             (log/warn
                               (u/format-color 'red (trs "All Metabase features may not work properly when using an unsupported version of Impala.")))))))

(defmethod driver/can-connect? :impala
  [driver details]
  ;; delegate to parent method to check whether we can connect; if so, check if it's an unsupported version and issue
  ;; a warning if it is
  (when ((get-method driver/can-connect? :sql-jdbc) driver details)
    (warn-on-unsupported-versions driver details)
    true))

;(defmethod driver/supports? [:impala :full-join] [_ _] false)

(defmethod driver/connection-properties :impala
  [_]
  (ssh/with-tunnel-config
    [driver.common/default-host-details
     (assoc driver.common/default-port-details :default 21050)
     (assoc driver.common/default-dbname-details :placeholder "default" :required true)
     ;driver.common/default-authmech-details
     ;driver.common/default-usenative-details
     ;(assoc driver.common/default-user-details :placeholder false)
     ;(assoc driver.common/default-password-details :placeholder false)
     driver.common/default-ssl-details
     (assoc driver.common/default-additional-options-details
       :placeholder  "KrbRealm=EXAMPLE.COM;KrbHostFQDN=impala.example.com;KrbServiceName=impala")
     ]))

(defmethod driver/date-add :impala
  [_ hsql-form amount unit]
  (hsql/call :date_add hsql-form (hsql/raw (format "INTERVAL %d %s" (int amount) (name unit)))))

(defmethod driver/humanize-connection-error-message :impala
  [_ message]
  (condp re-matches message
    #"^Communications link failure\s+The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.$"
    (driver.common/connection-error-messages :cannot-connect-check-host-and-port)

    #"^Unknown database .*$"
    (driver.common/connection-error-messages :database-name-incorrect)

    #"Access denied for user.*$"
    (driver.common/connection-error-messages :username-or-password-incorrect)

    #"Must specify port after ':' in connection string"
    (driver.common/connection-error-messages :invalid-hostname)

    #".*"                               ; default
    message))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql.qp/unix-timestamp->timestamp [:impala :seconds] [_ _ expr]
  (hsql/call :to_timestamp expr))

(defn- date-format [format-str expr] (hsql/call :date_format expr (hx/literal format-str)))
(defn- str-to-date [format-str expr] (hsql/call :str_to_date expr (hx/literal format-str)))

(defn- trunc
  "Truncate a datetime, also see:
   https://www.cloudera.com/documentation/enterprise/5-8-x/topics/impala_datetime_functions.html

      (trunc :day v) -> TRUNC(v, 'day')"
  [format-template v]
  (hsql/call :trunc v (hx/literal format-template)))

(defmethod sql.qp/date [:impala :default]         [_ _ expr] (hx/->timestamp expr))
(defmethod sql.qp/date [:impala :minute]          [_ _ expr] (trunc :MI expr))
(defmethod sql.qp/date [:impala :minute-of-hour]  [_ _ expr] (hsql/call :extract :minute expr))
(defmethod sql.qp/date [:impala :hour]            [_ _ expr] (trunc :HH expr))
(defmethod sql.qp/date [:impala :hour-of-day]     [_ _ expr] (hsql/call :extract :hour expr))
(defmethod sql.qp/date [:impala :day]             [_ _ expr] (trunc :dd expr))
(defmethod sql.qp/date [:impala :day-of-week]     [_ _ expr] (hsql/call :dayofweek expr))
(defmethod sql.qp/date [:impala :day-of-month]    [_ _ expr] (hsql/call :dayofmonth expr))
(defmethod sql.qp/date [:impala :day-of-year]     [_ _ expr] (hsql/call :dayofyear expr))
(defmethod sql.qp/date [:impala :week]     [_ _ expr] (trunc :day expr))
(defmethod sql.qp/date [:impala :week-of-year]     [_ _ expr] (hsql/call :weekofyear expr))
(defmethod sql.qp/date [:impala :month]     [_ _ expr] (trunc :month expr))
(defmethod sql.qp/date [:impala :month-of-year]   [_ _ expr] (hsql/call :extract :month expr))
(defmethod sql.qp/date [:impala :quarter]   [_ _ expr] (trunc :Q expr))
(defmethod sql.qp/date [:impala :quarter-of-year] [_ _ expr] (hx// (hx/+ (hsql/call :extract :month expr) 2) 3))
(defmethod sql.qp/date [:impala :year]            [_ _ expr] (hsql/call :extract :year expr))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         metabase.driver.sql-jdbc impls                                         |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.sync/database-type->base-type :impala
  [_ database-type]
  ({:INT        :type/Integer
    :STRING     :type/Text
    :ARRAY      :type/Text
    :BIGINT     :type/BigInteger
    :BINARY     :type/*
    :BOOLEAN    :type/Boolean
    :CHAR       :type/Text
    :DATE       :type/Date
    :DECIMAL    :type/Decimal
    :DOUBLE     :type/Float
    :FLOAT      :type/Float
    :MAP        :type/Text
    :SMALLINT   :type/Integer
    :STRUCT     :type/Text
    :TIMESTAMP  :type/DateTime
    :TINYINT    :type/Integer
    :VARCHAR    :type/Text}
   ;; strip off " UNSIGNED" from end if present
   (keyword (str/replace (name database-type) #"\sUNSIGNED$" ""))))

(def ^:private default-connection-args
  "Map of args for the JDBC connection string."
  { ;; 0000-00-00 dates are valid; convert these to `null` when they come back because they're illegal in Java
   :zeroDateTimeBehavior "convertToNull"
   ;; Force UTF-8 encoding of results
   :useUnicode           true
   :characterEncoding    "UTF8"
   :characterSetResults  "UTF8"
   :useCompression       true})

(defmethod sql-jdbc.conn/connection-details->spec :impala
  [_ {ssl? :ssl, :keys [additional-options], :as details}]
  ;; TODO - should this be fixed by a data migration instead?
  (let [ssl? (or ssl? (some-> additional-options (str/includes? "useSSL=true")))]
    (when (and ssl?
               (not (some->  additional-options (str/includes? "trustServerCertificate"))))
      (log/info (trs "You may need to add 'trustServerCertificate=true' to the additional connection options to connect with SSL.")))
    (merge
      default-connection-args
      {:useSSL (boolean ssl?)}
      (let [details (-> details
                        (set/rename-keys {:dbname :db})
                        (dissoc :ssl))]
        (-> (dbspec/impala details)
            (sql-jdbc.common/handle-additional-options details))))))

(def ^:private ^:const now (hsql/call :now))

(defn- date-interval [unit amount]
  (hsql/call :date_add
             :%now
             (hsql/raw (format "INTERVAL %d %s" (int amount) (name unit)))))

(defmethod sql-jdbc.sync/active-tables :impala
  [& args]
  (apply sql-jdbc.sync/specific-schema-active-tables args))

(defmethod sql-jdbc.sync/excluded-schemas :impala
  [_]
  #{"default" "_impala_builtins" "cloudera_manager_metastore_canary_test_db_hive_hivemetastore"})

(defmethod sql.qp/quote-style :impala [_] :impala)

(defmethod sql-jdbc.execute/set-timezone-sql :impala
  [_]
  "SET @@session.time_zone = %s;")

(defmethod sql-jdbc.execute/set-parameter [:impala OffsetTime]
  [driver ps i t]
  (sql-jdbc.execute/set-parameter driver ps i (t/local-time (t/with-offset-same-instant t (t/zone-offset 0)))))

;; TIMEZONE FIXME — not 100% sure this behavior makes sense
(defmethod sql-jdbc.execute/set-parameter [:impala OffsetDateTime]
  [driver ^java.sql.PreparedStatement ps ^Integer i t]
  (let [zone   (t/zone-id (qp.timezone/results-timezone-id))
        offset (.. zone getRules (getOffset (t/instant t)))
        t      (t/local-date-time (t/with-offset-same-instant t offset))]
    (sql-jdbc.execute/set-parameter driver ps i t)))

(defmethod sql-jdbc.execute/read-column [:impala Types/TIMESTAMP]
  [_ _ ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  (when-let [t (.getObject rs i Timestamp)]
    (if (= (.getColumnTypeName rsmeta i) "TIMESTAMP")
      (.toLocalDateTime t))))

(defn- format-offset [t]
  (let [offset (t/format "ZZZZZ" (t/zone-offset t))]
    (if (= offset "Z")
      "UTC"
      offset)))

(defmethod unprepare/unprepare-value [:impala OffsetTime]
  [_ t]
  (format "convert_tz('%s', '%s', @@session.time_zone)"
          (t/format "HH:mm:ss.SSS" t)
          (format-offset t)))

(defmethod unprepare/unprepare-value [:impala OffsetDateTime]
  [_ t]
  (format "convert_tz('%s', '%s', @@session.time_zone)"
          (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)
          (format-offset t)))

(defmethod unprepare/unprepare-value [:impala ZonedDateTime]
  [_ t]
  (format "convert_tz('%s', '%s', @@session.time_zone)"
          (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)
          (str (t/zone-id t))))

