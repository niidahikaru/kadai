#*******************************************************************************
#*      (C)2018 Sony Network Communications. All rights reserved.
#*
#* Title       : jtbb_count_subscriber_data
#*
#* Description : えらべる?楽部会員データ集計
#*
#* Database    : MASTER
#*
#* Tables      : オプションオーダー汎用テーブル (opta01)
#*               オプションステータスマスタテーブル  (optj01)
#*               会員マスタテーブル (tsna02)
#*
#* Stproc      :
#*
#* Authors     : Hikaru.Niida@so-net.co.jp
#*
#* History     : 2018/09/03 2018-100653 [JTBえらべる倶楽部（仮称）] First Version
#*
#* Output      :
#*
#*
#******************************************************************************
package jtbb_count_subscriber_data;

use strict;
use lib '/so-net/batch/common';
use lib '/so-net/batch/jtbb/lib/';
use SybaseLib;
use POSIX qw( mktime strftime );
use base qw( jtbb_common );


{
 # ---------------------------------------------------------------------------
 # メイン処理
 # ---------------------------------------------------------------------------
 sub batchMain {
   my ($obj) = @_;

   # 初期処理
   $obj->batchInit();

   # 対象データを取得
   my @get_data = $obj->getTargetData();

   # CSVファイル作成
   $obj->makeCSVData(@get_data);

   # 終了処理
   $obj->batchEnd(0);
 }
 # ---------------------------------------------------------------------------
 # 初期処理
 # ---------------------------------------------------------------------------
 sub batchInit {
   my ($obj) = @_;
   my @date = localtime($^T);

   # 日付/時刻情報をカレンダー時刻に変換
   my $date = mktime( 0, 0, 12, 15, $date[4], $date[5] );

   # --------------------------------------------
   # バッチ機能別定義
   # --------------------------------------------
   $obj->{'FUNC_NAME'} = 'jtbb_count_user';

   # JNW名、JOB名取得
   $obj->{'JNW_NAME'} = $ENV{'NSJNW_JNWNAME'};
   $obj->{'JOB_NAME'} = $ENV{'NSJNW_UJNAME'};

   ## 設定ファイル
   # JTBえらべる倶楽部 会員データ集計バッチ共通設定ファイル
   $jtbb_common::ENV_FILE = "$jtbb_common::BASE_DIR/$obj->{'FUNC_NAME'}/env/jtbb_count_user_common.env";

   # 終了メッセージファイル
   $jtbb_common::EXIT_MSG_FILE = "$jtbb_common::BASE_DIR/$obj->{'FUNC_NAME'}/env/jtbb_count_user_exit_msg.txt";

   # 設定ファイル読み込み ＆ 必須チェック
   $obj->{'conf_ary'} = $obj->readEnvFile($jtbb_common::ENV_FILE);             # 共通設定ファイルの読み込み
   #-------------------------- 日付設定処理----------------------------------
   # 処理開始年月日
   $obj->{'start_ymd'} = strftime("%Y%m", localtime($date)); # yyyymmdd

   # 処理開始実行時刻
   $obj->{'exec_time'} = strftime("%Y%m%d%H%M%S", localtime($^T)); # yyyymmddhhmmss

   # 当月設定
   $obj->{'this_month'} = strftime("%Y%m", localtime($date));           # yyyymm

   # 翌月設定
   $obj->{'next_month'} = strftime("%Y%m", localtime( $date + ( 60 * 60 * 24 * 30 )) );  # yyyymm

   # 前月設定
   $obj->{'last_month'} = strftime("%Y%m", localtime( $date - ( 60 * 60 * 24 * 30 )) );  # yyyymm

   #------------------ ----------------------------------------------------

   # ログレベル/ログファイル名設定(パラメータから指定)
   $obj->{'LOG_LEVEL'}  = $obj->{'conf_ary'}->{'LOG_LEVEL'};
   $obj->{'LOG_FILE'}   = $obj->makeLogFileName();

   # 集計用ファイル名設定(パラメータから指定)
   ($obj->{'INUSE_USER_CSV'}, $obj->{'CANCEL_USER_CSV'}) = $obj->makeCountFileName();

   # 処理終了メッセージのフォーマット
   $obj->{'END_MSG'} = "会員データ集計処理 %sEND\n";

   # --------------------------------------------
   # ログ初期化
   # --------------------------------------------
   $obj->logInit();

   # --------------------------------------------
   # 実行スクリプト＆ログファイル名出力
   # --------------------------------------------
   my $msg = "[実行スクリプト]$0\n";
   print($msg);
   $obj->{'log'}->info($msg);
   $msg = "ログファイル[$obj->{'LOG_FILE'}]\n";
   print($msg);
   $obj->{'log'}->info($msg);
   $msg = "ログレベル[$obj->{'LOG_LEVEL'}]\n";
   print($msg);
   $obj->{'log'}->info($msg);

   # 処理開始ログ出力
   $msg = "会員データ集計処理 START\n";
   print($msg);
   $obj->{'log'}->info($msg);

   # 初期化
   $obj->{'TOTAL_INUSE_USR'} = 0;
   $obj->{'TOTAL_CANCEL_USR'} = 0;

   # --------------------------------------------
   # DB接続
   # --------------------------------------------
   # Sybaseアクセスオブジェクト作成
   $obj->{'syb'} = new SybaseLib();
   # DB接続
   my $ret = $obj->{'syb'}->SybaseConnect($obj->{'conf_ary'}->{'DBNAME_MASTER'});
   if ($ret != 0) {
     $msg = sprintf("SybaseConnect() error! ret[%d] msg[%s]\n", $ret, $obj->{'syb'}->getErrMsg);
     print($msg);
     $obj->{'log'}->error($msg);
     $obj->batchEnd('10');
   }

 }

 # ---------------------------------------------------------------------------
 # 対象データ取得 / CSVファイル作成
 # ---------------------------------------------------------------------------
 sub getTargetData {
   my ($obj) = @_;

   # --------------------------------------------
   # データ取得
   # --------------------------------------------
   # SQL文作成
   my $sql  = "select 1 as usr_flag,";                                                 # ユーザー判定フラグ 1：利用中ユーザー
   $sql .= "       oa01.usr_id,";                                                      # システムID
   $sql .= "       ta02.usr_namej,";                                                   # 会員氏名 (漢字)
   $sql .= "       ta02.cnnct_mail,";                                                  # 連絡先メールアドレス
   $sql .= "       convert(char(10), oa01.srvc_start_ymd, 111),";                      # 利用開始年月日 yyyy/mm/dd
   $sql .= "       convert(char(10), oa01.srvc_end_ymd, 111)";                         # 利用終了年月日 yyyy/mm/dd
   $sql .= "  from tsnet01db..opta01 oa01,";                                           # オプションオーダー汎用テーブル
   $sql .= "       tsnet01db..optj01 oj01,";                                           # オプションステータスマスタテーブル
   $sql .= "       tsnet01db..tsna02 ta02";                                            # 会員マスタテーブル
   $sql .= " where oa01.opt_kbn = '026'";                                              # オプション区分 026：えらべる?楽部
   $sql .= "   and oa01.reg_sts = oj01.reg_sts";                                       # ステータス
   $sql .= "   and oj01.status_kbn = 'B'";                                             # 状態区分　B：開通(サービス開始)
   $sql .= "   and oa01.usr_id = ta02.usr_id ";                                         # システムID
   $sql .= "UNION All ";
   $sql .= "select 2 as usr_flag,";                                                    # ユーザー判定フラグ 2：当月解約ユーザー
   $sql .= "       oa01.usr_id,";                                                      # システムID
   $sql .= "       ta02.usr_namej,";                                                   # 会員氏名 (漢字)
   $sql .= "       ta02.cnnct_mail,";                                                  # 連絡先メールアドレス
   $sql .= "       convert(char(10), oa01.srvc_start_ymd, 111),";                      # 利用開始年月日 yyyy/mm/dd
   $sql .= "       convert(char(10), oa01.srvc_end_ymd, 111)";                         # 利用終了年月日 yyyy/mm/dd
   $sql .= "  from tsnet01db..opta01 oa01,";                                           # オプションオーダー汎用テーブル
   $sql .= "       tsnet01db..optj01 oj01,";                                           # オプションステータスマスタテーブル
   $sql .= "       tsnet01db..tsna02 ta02";                                            # 会員マスタテーブル
   $sql .= " where oa01.opt_kbn = '026'";                                              # オプション区分 026：えらべる?楽部
   $sql .= "   and oa01.reg_sts = oj01.reg_sts";                                       # ステータス
   $sql .= "   and oj01.status_kbn = 'D'";                                             # 状態区分　D：解約済み
   $sql .= "   and oa01.usr_id = ta02.usr_id";                                         # システムID
   $sql .= "   and convert(char(6), oa01.srvc_end_ymd, 112) < '$obj->{'next_month'}' ";   # 先月 < サービス終了日 < 翌月
   $sql .= "   and convert(char(6), oa01.srvc_end_ymd, 112) > '$obj->{'last_month'}' ";

   $obj->{'log'}->info("SQL = [$sql]\n");
   # SQL実行(3：複数行、複数カラム)
   my ($ret, @get_data) = $obj->{'syb'}->SybaseExec($sql, 3);

   # SQL実行エラー判定
   if ($ret != 0) {
     my $msg = sprintf("SybaseExec() error! ret[%d] msg[%s] sql[%s]\n", $ret, $obj->{'syb'}->getErrMsg, $sql);
     $obj->{'log'}->error($msg);
     $obj->batchEnd('15');
   }
   return @get_data;
 }

 # ---------------------------------------------------------------------------
 # CSVファイル作成
 # ---------------------------------------------------------------------------
 sub makeCSVData(){
   my ($obj, @get_data) = @_;

   # 出力先ディレクトリ
   my $csv_dir = sprintf("%s/%s", $obj->{'conf_ary'}->{'COUNT_DATA_OUTPUT_DIR'}, $obj->{'FUNC_NAME'});

   # ディレクトリ存在チェック
   if(!-d $csv_dir){
     $obj->batchEnd('33');
   }

   # 利用中ユーザー用CSVファイルオープン
   if(!open(INUSE_DATA, ">", "$obj->{'INUSE_USER_CSV'}")){
     my $msg = sprintf("Unable to open CSV file %s\n", $obj->{'INUSE_USER_CSV'});
     $obj->{'log'}->error($msg);
     $obj->batchEnd('32');
   }

   # 当月解約ユーザー用CSVファイルオープン
   if(!open(CANCEL_DATA, ">", "$obj->{'CANCEL_USER_CSV'}")){
     my $msg = sprintf("Unable to open CSV file %s\n", $obj->{'CANCEL_USER_CSV'});
     $obj->{'log'}->error($msg);
     $obj->batchEnd('32');
   }
   # --------------------------------------------
   # オーダ件数処理ループ開始
   # --------------------------------------------
   my %rows_data;

   for (my $idx = 0; $idx < @get_data; $idx++) {
     # --------------------------------------------
     # 取得した情報を設定
     # --------------------------------------------
     $rows_data{'usr_flag'}             = $get_data[$idx][0];
     $rows_data{'usr_id'}               = $get_data[$idx][1];
     $rows_data{'usr_namej'}            = $get_data[$idx][2];
     $rows_data{'cnnct_mail_address'}   = $get_data[$idx][3];
     $rows_data{'srvc_start_ymd'}       = $get_data[$idx][4];
     $rows_data{'srvc_end_ymd'}         = $get_data[$idx][5];

     my $line = sprintf( "%s\r\n", join( ',',
                                         $rows_data{'usr_id'},
                                         $rows_data{'usr_namej'},
                                         $rows_data{'cnnct_mail_address'},
                                         $rows_data{'srvc_start_ymd'},
                                         $rows_data{'srvc_end_ymd'} )
                                       );

    if($rows_data{'usr_flag'} eq '1'){
      # 利用中ユーザーの場合
      print INUSE_DATA $line;

      # 利用中ユーザー総件数カウント
      $obj->{'TOTAL_INUSE_USR'}++;
    }else{
      # 当月解約ユーザーの場合
      print CANCEL_DATA $line;

      # 当月解約ユーザー総件数カウント
      $obj->{'TOTAL_CANCEL_USR'}++;
    }

   } # 処理件数文ループ処理

   # ファイルクローズ処理
   close(INUSE_DATA);
   close(CANCEL_DATA);
 }


 # ---------------------------------------------------------------------------
 # ログファイル名設定関数(バッチ基底ディレクトリ下にログ作成)
 # ---------------------------------------------------------------------------
 sub makeLogFileName(){
   my ($obj) = @_;

   my $log_file_name = sprintf("%s/%s/%s_number_%s.log",
                               $jtbb_common::LOG_DIR,
                               $obj->{'FUNC_NAME'},
                               $obj->{'FUNC_NAME'},
                               $obj->{'exec_time'}
                       );

  return $log_file_name;
 }

 # ---------------------------------------------------------------------------
 # 集計結果ファイル名設定関数(バッチ基底ディレクトリ下に作成)
 # ---------------------------------------------------------------------------
 sub makeCountFileName(){
   my ($obj) = @_;

   # 利用中ユーザー用CSVファイル
   my $inuse_count_file = sprintf("%s/%s/inuse_%s.csv",
                                  $obj->{'conf_ary'}->{'COUNT_DATA_OUTPUT_DIR'},
                                  $obj->{'FUNC_NAME'},
                                  $obj->{'this_month'}      # yyyymm
                                 );

   # 当月解約ユーザー用CSVファイル
   my $cancel_count_file = sprintf("%s/%s/cancel_%s.csv",
                                   $obj->{'conf_ary'}->{'COUNT_DATA_OUTPUT_DIR'},
                                   $obj->{'FUNC_NAME'},
                                   $obj->{'this_month'}     # yyyymm
                                  );

   return $inuse_count_file, $cancel_count_file;
 }

 # ---------------------------------------------------------------------------
 # 終了前処理(batchEndの最初に呼ばれる)
 # ---------------------------------------------------------------------------
 sub beforeBatchEnd(){
   my ($obj) = @_;
   # --------------------------------------------
   # 集計ログ出力
   # --------------------------------------------
   if($obj->{'log'}){
     my $msg = "えらべる倶楽部利用中ユーザー総件数：$obj->{'TOTAL_INUSE_USR'}件\n";
     # 利用中ユーザー総件数標準出力
     print($msg);
     $obj->{'log'}->info($msg);

     $msg = "えらべる倶楽部当月解約ユーザー総件数：$obj->{'TOTAL_CANCEL_USR'}件\n";
     # 当月解約ユーザー総件数標準出力
     print($msg);
     $obj->{'log'}->info($msg);
   }
 }

}
#---------------------------------------------------------------
# バッチ実行
#---------------------------------------------------------------
jtbb_count_subscriber_data->new->batchMain;
