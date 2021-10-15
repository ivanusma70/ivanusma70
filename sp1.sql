USE [Reports]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [jsp_UserAccessSummary] 
			 @pClientID     INT
		    , @pEmployeeType VARCHAR(15)
		    , @pEmployedBy   VARCHAR(30)
		    , @pWorkGroupID  INT
		    , @pActive       VARCHAR(13)
		    , @pTerminate    VARCHAR(13)
AS
    BEGIN
	   BEGIN TRY
/*****************
Tracking variables
*****************/
		  DECLARE 
			    @ProcLogID INT
			  , @SchName   VARCHAR(128)=OBJECT_SCHEMA_NAME(@@PROCID)
			  , @ProcName  VARCHAR(128)=OBJECT_NAME(@@PROCID)
			  , @Params VARCHAR(MAX)='@pClientID= ' + ISNULL(CAST(@pClientID AS VARCHAR), '') + ', @pEmployeeType= ' + ISNULL(@pEmployeeType, '') + ', @pEmployedBy= ' + ISNULL(@pEmployedBy, '') + ', @pWorkGroupID= ' + ISNULL(CAST(@pWorkGroupID AS VARCHAR), '') + ', @pActive= ' + ISNULL(@pActive, '') + ', @pTerminate= ' + ISNULL(@pTerminate, '');
/**************
Insert Tracking
**************/
		  EXEC crud.sp_ProcLogInsert 
			  @SchName=@SchName, 
			  @ProcName=@ProcName, 
			  @Params=@Params, 
			  @myout=@ProcLogID OUTPUT;
/*************
Procedure code
*************/
/*************************
EXEC jsp_UserAccessSummary
	@pClientID=1,
	@pEmployeeType='Client',
	@pEmployedBy='ALL',
	@pWorkGroupID=0,
	@pActive='Disabled Only',
	@pTerminate='All';
*************************/
		  SET NOCOUNT ON;
		  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
/****************************
Remap parameters to variables
****************************/
		  DECLARE 
			    @vClientID     INT        =@pClientID
			  , @vEmployeeType VARCHAR(15)=@pEmployeeType
			  , @vEmployedBy   VARCHAR(30)=@pEmployedBy
			  , @vWorkGroupID  INT        =@pWorkGroupID
			  , @vActive       VARCHAR(13)=@pActive
			  , @vTerminate    VARCHAR(13)=@pTerminate;
/****************************************************************************************************
Store variables in table so I can filter the later query in an inner join instead of the where clause
****************************************************************************************************/
		  DECLARE 
			    @mytable TABLE(
						    isactive INT
						  , emptyp   INT
						  , empby    VARCHAR(30)
						  , wid      INT);
		  INSERT INTO         @mytable(
			    isactive
			  , emptyp
			  , empby
			  , wid)
		  SELECT 
			    CASE
				    WHEN @vActive LIKE '%enabled%'
					    THEN 1
				    WHEN @vActive LIKE '%Disabled%'
					    THEN 0
			    END AS isactive
			  , CASE
				    WHEN @vEmployeeType LIKE '%Emp%'
					    THEN 1
				    WHEN @vEmployeeType LIKE '%Client%'
					    THEN 2
			    END AS emptyp
			  , CASE
				    WHEN
					    @vEmployedBy <> 'ALL'
					    THEN @vEmployedBy
				    ELSE NULL
			    END AS empby
			  , CASE
				    WHEN
					    @vWorkGroupID <> 0
					    THEN @vWorkGroupID
				    ELSE NULL
			    END AS wid;
		  WITH All_Rows
			  AS (SELECT DISTINCT 
					   au.AppUserID AS                                                          AppUserID_spUserAccessSum
					 , grp.workgroupid AS                                                       workgroupid_spUserAccessSum
					 , au.EmployeeTypeID AS                                                     EmployeeTypeID_spUserAccessSum
					 , CASE
						   WHEN
							   au.EmployeeTypeID = 1
							   THEN 'ProCare Rx'
						   ELSE 'Client'
					   END AS                                                                   EmployeeType_spUserAccessSum
					 , au.EmployedBy AS                                                         EmployedBy_spUserAccessSum
					 , au.FirstName AS                                                          FirstName_spUserAccessSum
					 , au.LastName AS                                                           LastName_spUserAccessSum
					 , TRIM(ISNULL(au.LastName, '')) + ', ' + TRIM(ISNULL(au.FirstName, '')) AS EmployeeName_spUserAccessSum
					 , au.LogonID AS                                                            LogonID_spUserAccessSum
					 , au.EffDate AS                                                            EffDate_spUserAccessSum
					 , (CASE
						    WHEN
							    YEAR(au.TrmDate) = 9999
							    THEN NULL
						    ELSE au.TrmDate
					    END) AS                                                                 TrmDate_spUserAccessSum
					 , TRIM(grp.WorkGroupName) AS                                               WorkGroupName_spUserAccessSum
					 , TRIM(grp.WorkGroupDescription) AS                                        WorkGroupDesc_spUserAccessSum
					 , au.LogonAttemptDate AS                                                   LogonAttemptDate_spUserAccessSum
					 , au.PWChangeDate AS                                                       PWChangeDate_spUserAccessSum
					 , CASE
						   WHEN au.PWChangeDate IS NULL
							   THEN 'Yes'
						   WHEN
							   au.PWChangeDate < DATEADD(dd, -90, GETDATE())
							   THEN 'Yes'
						   ELSE 'No'
					   END AS                                                                   IsPWChangeRequired_spUserAccessSum
					 , DENSE_RANK() OVER(PARTITION BY au.LogonID
					   ORDER BY 
							  CASE grp.WorkGroupName
								  WHEN 'ALL PARENTS'
									  THEN 1
								  ELSE 2
							  END
							, TRIM(grp.WorkGroupName)) AS                                     RowNo_spUserAccessSum
				 FROM   
					 [SQLSERVER].dbo.APPUSER AS au
					 INNER JOIN @mytable AS m
						 ON
						    au.EmployeeTypeID = COALESCE(m.emptyp, au.EmployeeTypeID)
						    AND
							   au.clientid = @vClientID
						    AND (
							   au.EmployedBy = m.empby
							   OR m.empby IS NULL)
						    AND (
							   au.isActive = m.isactive
							   OR m.isactive IS NULL)
						    AND ((@vTerminate LIKE '%Active%'
								AND ((
									au.trmdate > GETDATE())
									OR au.trmdate IS NULL))
							    OR (@vTerminate LIKE '%inactive%'
								   AND
									  au.trmdate < GETDATE())
							    OR (@vTerminate LIKE '%ALL%'))
					 LEFT JOIN [SQL].PRXDW_Prod.dbo.APPUSERWORKGROUP AS wgrp
						 ON
						    au.appuserid = wgrp.appuserid
						    AND
							   wgrp.IsGranted = 1
						    AND
							   au.ClientID = wgrp.ClientID
					 LEFT JOIN [SQL].PRXDW_Prod.dbo.WORKGROUP AS grp
						 ON
						    wgrp.workgroupid = grp.workgroupid
						    AND
							   au.ClientID = grp.clientid
				 WHERE  1 = 1
					   AND (m.wid IS NULL
						   OR
							 m.wid = grp.Workgroupid)),
			  FirstRows
			  AS (SELECT 
					   ar.AppUserID_spUserAccessSum
					 , ar.EmployedBy_spUserAccessSum
					 , ar.EmployeeName_spUserAccessSum
					 , ar.LogonID_spUserAccessSum
					 , ar.WorkGroupName_spUserAccessSum
				 FROM   
					 All_Rows AS ar
				 WHERE
					   ar.RowNo_spUserAccessSum = 1)
			  SELECT 
				    ar.AppUserID_spUserAccessSum
				  , ar.workgroupid_spUserAccessSum
				  , ar.EmployeeTypeID_spUserAccessSum
				  , ar.EmployeeType_spUserAccessSum
				  , ar.EmployedBy_spUserAccessSum
				  , ar.FirstName_spUserAccessSum
				  , ar.LastName_spUserAccessSum
				  , ar.EmployeeName_spUserAccessSum
				  , ar.LogonID_spUserAccessSum
				  , ar.EffDate_spUserAccessSum
				  , ar.TrmDate_spUserAccessSum
				  , ar.WorkGroupName_spUserAccessSum
				  , ar.WorkGroupDesc_spUserAccessSum
				  , ar.LogonAttemptDate_spUserAccessSum
				  , ar.PWChangeDate_spUserAccessSum
				  , ar.IsPWChangeRequired_spUserAccessSum
				  , ar.RowNo_spUserAccessSum
			  FROM   
				  All_Rows AS ar
				  INNER JOIN FirstRows AS fr
					  ON
						ar.AppUserID_spUserAccessSum = fr.AppUserID_spUserAccessSum
						AND
						    ar.EmployedBy_spUserAccessSum = fr.EmployedBy_spUserAccessSum
						AND
						    ar.EmployeeName_spUserAccessSum = fr.EmployeeName_spUserAccessSum
						AND
						    ar.LogonID_spUserAccessSum = fr.LogonID_spUserAccessSum
						AND ((
							ar.WorkGroupName_spUserAccessSum = fr.WorkGroupName_spUserAccessSum
							AND
							    ar.WorkGroupName_spUserAccessSum = 'ALL PARENTS')
							OR
							   fr.WorkGroupName_spUserAccessSum <> 'ALL PARENTS')
			  ORDER BY 
					 ar.EmployeeType_spUserAccessSum DESC
				    , ar.EmployedBy_spUserAccessSum
				    , ar.EmployeeName_spUserAccessSum
				    , ar.LogonID_spUserAccessSum
				    , ar.RowNo_spUserAccessSum;
/********************
Tracking set end time
********************/
		  UPDATE plog
		    SET  
			   plog.EndTime=CURRENT_TIMESTAMP
		  FROM   CRUD.ProcLog plog
		  WHERE  
			   plog.ProcLogID = @ProcLogID;
	   END TRY
/********************
Tracking Catch Errors
********************/
	   BEGIN CATCH
		  UPDATE plog
		    SET  
			   plog.EndTime=CURRENT_TIMESTAMP, 
			   plog.ErrProc=ERROR_PROCEDURE(), 
			   plog.ErrLine=ERROR_LINE(), 
			   plog.ErrMsg=ERROR_MESSAGE(), 
			   plog.ErrNum=ERROR_NUMBER()
		  FROM   CRUD.ProcLog plog
		  WHERE  
			   plog.ProcLogID = @ProcLogID;
	   END CATCH;
    END;
