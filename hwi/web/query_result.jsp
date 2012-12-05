<%--
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
--%>
<%@page import="org.apache.hadoop.hive.hwi.*" %>
<%@page import="org.apache.hadoop.hive.hwi.model.MQuery"%>
<%@page import="org.apache.hadoop.hive.conf.HiveConf"%>
<%@page import="org.apache.hadoop.hive.ql.session.SessionState"%>
<%@page errorPage="error_page.jsp" %>
<%
    String idStr = request.getParameter("id");
    Integer id = null;
    try {
        id = Integer.parseInt(idStr);
    } catch (Exception e) {
        
    }

	HiveConf hiveConf = new HiveConf(SessionState.class);
	QueryStore qs = new QueryStore(hiveConf);
	MQuery mquery = qs.getById(id);
	
	String name = mquery.getName();
	String query = mquery.getQuery();
	String resultLocation = mquery.getResultLocation();
	String callback = mquery.getCallback();
	
    String message = null;
    String action = request.getParameter("action");
    String errmsg = mquery.getErrorMsg();
    
    MQuery.Status status = mquery.getStatus();
%>
<!DOCTYPE html>
<html>
<head>
<title>Query Result <%=name%></title>
<link href="css/bootstrap.min.css" rel="stylesheet">
</head>
<body style="padding-top: 60px;">
    <jsp:include page="/navbar.jsp"></jsp:include>
	<div class="container">
		<div class="row">
			<div class="span4">
				<jsp:include page="/left_navigation.jsp" />
			</div><!-- span4 -->
			<div class="span8">
				<h2><%=name%> Query Result</h2>

				<% if (message != null) {  %>
				<div class="alert alert-info"><%= message %></div>
				<% } %>

                <% if (status == MQuery.Status.RUNNING) { %>
				<div class="alert alert-warning">
				Session is in QUERY_RUNNING state. Changes are not possible!
				</div>
				<% } %>

			</div><!-- span8 -->
		</div><!-- row -->
	</div><!-- container -->
</body>
</html>