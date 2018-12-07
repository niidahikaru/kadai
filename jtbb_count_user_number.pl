#*******************************************************************************
#*      (C)2018 Sony Network Communications. All rights reserved.
#*
#* Title       : jtbb_count_subscriber_data
#*
#* Description : ����ׂ�?�y������f�[�^�W�v
#*
#* Database    : MASTER
#*
#* Tables      : �I�v�V�����I�[�_�[�ėp�e�[�u�� (opta01)
#*               �I�v�V�����X�e�[�^�X�}�X�^�e�[�u��  (optj01)
#*               ����}�X�^�e�[�u�� (tsna02)
#*
#* Stproc      :
#*
#* Authors     : Hikaru.Niida@so-net.co.jp
#*
#* History     : 2018/09/03 2018-100653 [JTB����ׂ��y���i���́j] First Version
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
 # ���C������
 # ---------------------------------------------------------------------------
 sub batchMain {
   my ($obj) = @_;

   # ��������
   $obj->batchInit();

   # �Ώۃf�[�^���擾
   my @get_data = $obj->getTargetData();

   # CSV�t�@�C���쐬
   $obj->makeCSVData(@get_data);

   # �I������
   $obj->batchEnd(0);
 }
 # ---------------------------------------------------------------------------
 # ��������
 # ---------------------------------------------------------------------------
 sub batchInit {
   my ($obj) = @_;
   my @date = localtime($^T);

   # ���t/���������J�����_�[�����ɕϊ�
   my $date = mktime( 0, 0, 12, 15, $date[4], $date[5] );

   # --------------------------------------------
   # �o�b�`�@�\�ʒ�`
   # --------------------------------------------
   $obj->{'FUNC_NAME'} = 'jtbb_count_user';

   # JNW���AJOB���擾
   $obj->{'JNW_NAME'} = $ENV{'NSJNW_JNWNAME'};
   $obj->{'JOB_NAME'} = $ENV{'NSJNW_UJNAME'};

   ## �ݒ�t�@�C��
   # JTB����ׂ��y�� ����f�[�^�W�v�o�b�`���ʐݒ�t�@�C��
   $jtbb_common::ENV_FILE = "$jtbb_common::BASE_DIR/$obj->{'FUNC_NAME'}/env/jtbb_count_user_common.env";

   # �I�����b�Z�[�W�t�@�C��
   $jtbb_common::EXIT_MSG_FILE = "$jtbb_common::BASE_DIR/$obj->{'FUNC_NAME'}/env/jtbb_count_user_exit_msg.txt";

   # �ݒ�t�@�C���ǂݍ��� �� �K�{�`�F�b�N
   $obj->{'conf_ary'} = $obj->readEnvFile($jtbb_common::ENV_FILE);             # ���ʐݒ�t�@�C���̓ǂݍ���
   #-------------------------- ���t�ݒ菈��----------------------------------
   # �����J�n�N����
   $obj->{'start_ymd'} = strftime("%Y%m", localtime($date)); # yyyymmdd

   # �����J�n���s����
   $obj->{'exec_time'} = strftime("%Y%m%d%H%M%S", localtime($^T)); # yyyymmddhhmmss

   # �����ݒ�
   $obj->{'this_month'} = strftime("%Y%m", localtime($date));           # yyyymm

   # �����ݒ�
   $obj->{'next_month'} = strftime("%Y%m", localtime( $date + ( 60 * 60 * 24 * 30 )) );  # yyyymm

   # �O���ݒ�
   $obj->{'last_month'} = strftime("%Y%m", localtime( $date - ( 60 * 60 * 24 * 30 )) );  # yyyymm

   #------------------ ----------------------------------------------------

   # ���O���x��/���O�t�@�C�����ݒ�(�p�����[�^����w��)
   $obj->{'LOG_LEVEL'}  = $obj->{'conf_ary'}->{'LOG_LEVEL'};
   $obj->{'LOG_FILE'}   = $obj->makeLogFileName();

   # �W�v�p�t�@�C�����ݒ�(�p�����[�^����w��)
   ($obj->{'INUSE_USER_CSV'}, $obj->{'CANCEL_USER_CSV'}) = $obj->makeCountFileName();

   # �����I�����b�Z�[�W�̃t�H�[�}�b�g
   $obj->{'END_MSG'} = "����f�[�^�W�v���� %sEND\n";

   # --------------------------------------------
   # ���O������
   # --------------------------------------------
   $obj->logInit();

   # --------------------------------------------
   # ���s�X�N���v�g�����O�t�@�C�����o��
   # --------------------------------------------
   my $msg = "[���s�X�N���v�g]$0\n";
   print($msg);
   $obj->{'log'}->info($msg);
   $msg = "���O�t�@�C��[$obj->{'LOG_FILE'}]\n";
   print($msg);
   $obj->{'log'}->info($msg);
   $msg = "���O���x��[$obj->{'LOG_LEVEL'}]\n";
   print($msg);
   $obj->{'log'}->info($msg);

   # �����J�n���O�o��
   $msg = "����f�[�^�W�v���� START\n";
   print($msg);
   $obj->{'log'}->info($msg);

   # ������
   $obj->{'TOTAL_INUSE_USR'} = 0;
   $obj->{'TOTAL_CANCEL_USR'} = 0;

   # --------------------------------------------
   # DB�ڑ�
   # --------------------------------------------
   # Sybase�A�N�Z�X�I�u�W�F�N�g�쐬
   $obj->{'syb'} = new SybaseLib();
   # DB�ڑ�
   my $ret = $obj->{'syb'}->SybaseConnect($obj->{'conf_ary'}->{'DBNAME_MASTER'});
   if ($ret != 0) {
     $msg = sprintf("SybaseConnect() error! ret[%d] msg[%s]\n", $ret, $obj->{'syb'}->getErrMsg);
     print($msg);
     $obj->{'log'}->error($msg);
     $obj->batchEnd('10');
   }

 }

 # ---------------------------------------------------------------------------
 # �Ώۃf�[�^�擾 / CSV�t�@�C���쐬
 # ---------------------------------------------------------------------------
 sub getTargetData {
   my ($obj) = @_;

   # --------------------------------------------
   # �f�[�^�擾
   # --------------------------------------------
   # SQL���쐬
   my $sql  = "select 1 as usr_flag,";                                                 # ���[�U�[����t���O 1�F���p�����[�U�[
   $sql .= "       oa01.usr_id,";                                                      # �V�X�e��ID
   $sql .= "       ta02.usr_namej,";                                                   # ������� (����)
   $sql .= "       ta02.cnnct_mail,";                                                  # �A���惁�[���A�h���X
   $sql .= "       convert(char(10), oa01.srvc_start_ymd, 111),";                      # ���p�J�n�N���� yyyy/mm/dd
   $sql .= "       convert(char(10), oa01.srvc_end_ymd, 111)";                         # ���p�I���N���� yyyy/mm/dd
   $sql .= "  from tsnet01db..opta01 oa01,";                                           # �I�v�V�����I�[�_�[�ėp�e�[�u��
   $sql .= "       tsnet01db..optj01 oj01,";                                           # �I�v�V�����X�e�[�^�X�}�X�^�e�[�u��
   $sql .= "       tsnet01db..tsna02 ta02";                                            # ����}�X�^�e�[�u��
   $sql .= " where oa01.opt_kbn = '026'";                                              # �I�v�V�����敪 026�F����ׂ�?�y��
   $sql .= "   and oa01.reg_sts = oj01.reg_sts";                                       # �X�e�[�^�X
   $sql .= "   and oj01.status_kbn = 'B'";                                             # ��ԋ敪�@B�F�J��(�T�[�r�X�J�n)
   $sql .= "   and oa01.usr_id = ta02.usr_id ";                                         # �V�X�e��ID
   $sql .= "UNION All ";
   $sql .= "select 2 as usr_flag,";                                                    # ���[�U�[����t���O 2�F������񃆁[�U�[
   $sql .= "       oa01.usr_id,";                                                      # �V�X�e��ID
   $sql .= "       ta02.usr_namej,";                                                   # ������� (����)
   $sql .= "       ta02.cnnct_mail,";                                                  # �A���惁�[���A�h���X
   $sql .= "       convert(char(10), oa01.srvc_start_ymd, 111),";                      # ���p�J�n�N���� yyyy/mm/dd
   $sql .= "       convert(char(10), oa01.srvc_end_ymd, 111)";                         # ���p�I���N���� yyyy/mm/dd
   $sql .= "  from tsnet01db..opta01 oa01,";                                           # �I�v�V�����I�[�_�[�ėp�e�[�u��
   $sql .= "       tsnet01db..optj01 oj01,";                                           # �I�v�V�����X�e�[�^�X�}�X�^�e�[�u��
   $sql .= "       tsnet01db..tsna02 ta02";                                            # ����}�X�^�e�[�u��
   $sql .= " where oa01.opt_kbn = '026'";                                              # �I�v�V�����敪 026�F����ׂ�?�y��
   $sql .= "   and oa01.reg_sts = oj01.reg_sts";                                       # �X�e�[�^�X
   $sql .= "   and oj01.status_kbn = 'D'";                                             # ��ԋ敪�@D�F���ς�
   $sql .= "   and oa01.usr_id = ta02.usr_id";                                         # �V�X�e��ID
   $sql .= "   and convert(char(6), oa01.srvc_end_ymd, 112) < '$obj->{'next_month'}' ";   # �挎 < �T�[�r�X�I���� < ����
   $sql .= "   and convert(char(6), oa01.srvc_end_ymd, 112) > '$obj->{'last_month'}' ";

   $obj->{'log'}->info("SQL = [$sql]\n");
   # SQL���s(3�F�����s�A�����J����)
   my ($ret, @get_data) = $obj->{'syb'}->SybaseExec($sql, 3);

   # SQL���s�G���[����
   if ($ret != 0) {
     my $msg = sprintf("SybaseExec() error! ret[%d] msg[%s] sql[%s]\n", $ret, $obj->{'syb'}->getErrMsg, $sql);
     $obj->{'log'}->error($msg);
     $obj->batchEnd('15');
   }
   return @get_data;
 }

 # ---------------------------------------------------------------------------
 # CSV�t�@�C���쐬
 # ---------------------------------------------------------------------------
 sub makeCSVData(){
   my ($obj, @get_data) = @_;

   # �o�͐�f�B���N�g��
   my $csv_dir = sprintf("%s/%s", $obj->{'conf_ary'}->{'COUNT_DATA_OUTPUT_DIR'}, $obj->{'FUNC_NAME'});

   # �f�B���N�g�����݃`�F�b�N
   if(!-d $csv_dir){
     $obj->batchEnd('33');
   }

   # ���p�����[�U�[�pCSV�t�@�C���I�[�v��
   if(!open(INUSE_DATA, ">", "$obj->{'INUSE_USER_CSV'}")){
     my $msg = sprintf("Unable to open CSV file %s\n", $obj->{'INUSE_USER_CSV'});
     $obj->{'log'}->error($msg);
     $obj->batchEnd('32');
   }

   # ������񃆁[�U�[�pCSV�t�@�C���I�[�v��
   if(!open(CANCEL_DATA, ">", "$obj->{'CANCEL_USER_CSV'}")){
     my $msg = sprintf("Unable to open CSV file %s\n", $obj->{'CANCEL_USER_CSV'});
     $obj->{'log'}->error($msg);
     $obj->batchEnd('32');
   }
   # --------------------------------------------
   # �I�[�_�����������[�v�J�n
   # --------------------------------------------
   my %rows_data;

   for (my $idx = 0; $idx < @get_data; $idx++) {
     # --------------------------------------------
     # �擾��������ݒ�
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
      # ���p�����[�U�[�̏ꍇ
      print INUSE_DATA $line;

      # ���p�����[�U�[�������J�E���g
      $obj->{'TOTAL_INUSE_USR'}++;
    }else{
      # ������񃆁[�U�[�̏ꍇ
      print CANCEL_DATA $line;

      # ������񃆁[�U�[�������J�E���g
      $obj->{'TOTAL_CANCEL_USR'}++;
    }

   } # �������������[�v����

   # �t�@�C���N���[�Y����
   close(INUSE_DATA);
   close(CANCEL_DATA);
 }


 # ---------------------------------------------------------------------------
 # ���O�t�@�C�����ݒ�֐�(�o�b�`���f�B���N�g�����Ƀ��O�쐬)
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
 # �W�v���ʃt�@�C�����ݒ�֐�(�o�b�`���f�B���N�g�����ɍ쐬)
 # ---------------------------------------------------------------------------
 sub makeCountFileName(){
   my ($obj) = @_;

   # ���p�����[�U�[�pCSV�t�@�C��
   my $inuse_count_file = sprintf("%s/%s/inuse_%s.csv",
                                  $obj->{'conf_ary'}->{'COUNT_DATA_OUTPUT_DIR'},
                                  $obj->{'FUNC_NAME'},
                                  $obj->{'this_month'}      # yyyymm
                                 );

   # ������񃆁[�U�[�pCSV�t�@�C��
   my $cancel_count_file = sprintf("%s/%s/cancel_%s.csv",
                                   $obj->{'conf_ary'}->{'COUNT_DATA_OUTPUT_DIR'},
                                   $obj->{'FUNC_NAME'},
                                   $obj->{'this_month'}     # yyyymm
                                  );

   return $inuse_count_file, $cancel_count_file;
 }

 # ---------------------------------------------------------------------------
 # �I���O����(batchEnd�̍ŏ��ɌĂ΂��)
 # ---------------------------------------------------------------------------
 sub beforeBatchEnd(){
   my ($obj) = @_;
   # --------------------------------------------
   # �W�v���O�o��
   # --------------------------------------------
   if($obj->{'log'}){
     my $msg = "����ׂ��y�����p�����[�U�[�������F$obj->{'TOTAL_INUSE_USR'}��\n";
     # ���p�����[�U�[�������W���o��
     print($msg);
     $obj->{'log'}->info($msg);

     $msg = "����ׂ��y��������񃆁[�U�[�������F$obj->{'TOTAL_CANCEL_USR'}��\n";
     # ������񃆁[�U�[�������W���o��
     print($msg);
     $obj->{'log'}->info($msg);
   }
 }

}
#---------------------------------------------------------------
# �o�b�`���s
#---------------------------------------------------------------
jtbb_count_subscriber_data->new->batchMain;
