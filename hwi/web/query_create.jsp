<%--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
--%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.util.TimeZone"%>
<%@page import="java.util.Calendar"%>
<%@page import="java.util.Date"%>
<%@page import="java.text.SimpleDateFormat" %>
<%@page import="java.util.concurrent.ThreadPoolExecutor" %>
<%@page import="com.sun.org.apache.bcel.internal.generic.NEW"%>
<%@page import="org.apache.hadoop.hive.hwi.model.MQuery"%>
<%@page import="org.apache.hadoop.hive.hwi.*"%>
<%@page import="org.apache.hadoop.hive.conf.HiveConf"%>
<%@page import="org.apache.hadoop.hive.ql.session.SessionState"%>
<%@page errorPage="error_page.jsp"%>
<%
    
    String action = request.getParameter("action");
    
    if ("add_query".equals(action)) {
        String queryName = request.getParameter("queryName");
        String query = request.getParameter("query");
        String callback = request.getParameter("callback");
        
        Date created = Calendar.getInstance(TimeZone.getDefault()).getTime();
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
        
        if (queryName == null || "".equals(queryName)) {
            queryName = sf.format(created);
        }
        
        HiveConf hiveConf = new HiveConf(SessionState.class);
        QueryStore qs = new QueryStore(hiveConf);
        
        MQuery mquery = new MQuery();
        mquery.setName(queryName);
        mquery.setQuery(query);
        mquery.setCallback(callback);
        mquery.setCreated(created);
        mquery.setUpdated(created);
        mquery.setStatus(MQuery.Status.INITED);
        mquery.setResultLocation("");
        mquery.setUserId("hadoop");
        qs.insertQuery(mquery);
        
        mquery.setResultLocation("/tmp/" + mquery.getId() + "/");
        qs.updateQuery(mquery);
        
        // submit to threadpool
        ThreadPoolExecutor executor = (ThreadPoolExecutor) application.getAttribute("exector");
        executor.execute(new QueryWorker(mquery));
        
	    //RequestDispatcher rd = application.getRequestDispatcher("query_manage.jsp?id=" + mquery.getId());
	    //rd.forward(request, response);
	    response.sendRedirect("query_info.jsp?id=" + mquery.getId());
    }
%>
<!DOCTYPE html>
<html>
<head>
<title>Create a Hive Query - Hive Web Interface</title>
<link href="css/bootstrap.min.css" rel="stylesheet">
<style type="text/css">
#fldquery {
font-family: Menlo,Monaco,"Courier New",monospace;
}
</style>
</head>
<body style="padding-top: 60px;">
    <jsp:include page="/navbar.jsp"></jsp:include>
	<div class="container">
		<div class="row">
			<div class="span2">
				<jsp:include page="/left_navigation.jsp" />
			</div><!-- span2 -->
			
			<div class="span10">
				<form action="" method="post" class="form-horizontal">
					<fieldset>
						<legend>Create a Hive Query</legend>
						
						<div class="control-group">
                            <label class="control-label" for="fldquery">Query</label>
                            <div class="controls">
                            <textarea id="fldquery" class="input-block-level" name="query" rows="10" cols="20" placeholder="Enter hive query"></textarea>
                            </div>
                        </div>
                        
                        <div class="control-group">
                            <label class="control-label" for="fldquery">Query Name</label>
                            <div class="controls">
                                <input id="fldquery" type="text" name="queryName" value="" placeholder="Enter query name" />
                            </div>
                        </div>
                        <!-- 
                        <div class="control-group">
                            <label class="control-label" for="fldcallback">Callback</label>
                            <div class="controls">
                                <input id="fldcallback" type="text" name="callback" value="" />
                            </div>
                        </div>
                         -->
					</fieldset>

					<div class="form-actions">
					<input type="hidden" name="action" value="add_query" />
						<button type="submit" class="btn btn-primary">Submit</button>
					</div>
				</form>

			</div><!-- span8 -->
		</div><!-- row -->
	</div><!-- container -->
</body>
</html>
