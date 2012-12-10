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
<%@page import="java.io.InputStreamReader"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.BufferedInputStream"%>
<%@page import="org.apache.hadoop.fs.Path"%>
<%@page import="org.apache.hadoop.fs.FileSystem"%>
<%@page import="org.apache.hadoop.hive.hwi.*" %>
<%@page import="org.apache.hadoop.hive.hwi.model.MQuery"%>
<%@page import="org.apache.hadoop.hive.conf.HiveConf" %>
<%@page import="org.apache.hadoop.fs.FSDataInputStream"%>
<%@page import="org.apache.hadoop.hive.ql.session.SessionState"%>
<%@page import="org.apache.hadoop.fs.FileStatus" %>
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
			<div class="span2">
				<jsp:include page="/left_navigation.jsp" />
			</div><!-- span2 -->
			<div class="span10">
				<h4><%=name%> Query Result</h4>

				<% if (message != null) {  %>
				<div class="alert alert-info"><%= message %></div>
				<% } %>

                <% if (status == MQuery.Status.FINISHED) {
                
                    String temp, resultStr="";
                    Path rPath = new Path(resultLocation);
                    FileSystem fs = rPath.getFileSystem(hiveConf);
                    
                    if (fs.getFileStatus(rPath).isDir()) {
                        FileStatus[] fss = fs.listStatus(rPath);
                        for (FileStatus _fs : fss) {
                            if (!fs.getFileStatus(_fs.getPath()).isDir()) {
                                BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(_fs.getPath())));
                                
                                while ((temp = bf.readLine()) != null) {
                                    resultStr += ( temp.replace('\1', '\t') + "\n");        
                                }
                                bf.close();
                            }
                        }
                    }
                    FileSystem.closeAll();
                %>
                <div><pre><%= resultStr %></pre></div>
                <% } else { %>
				<div class="alert alert-warning">
				Session is not in FINISHED status. Result are not exists!
				</div>
				<% } %>

			</div><!-- span10 -->
		</div><!-- row -->
	</div><!-- container -->
</body>
</html>