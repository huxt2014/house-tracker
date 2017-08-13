# coding=utf-8

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from flask import (render_template, redirect, url_for, jsonify, request, abort)
from sqlalchemy import select, func

from house_tracker.models import *
from . import app


def my_jsonify(status="200", **kwargs):
    resp = jsonify(**kwargs)
    resp.status = status
    return resp


@app.route('/')
def index():
    return redirect(url_for('lianjia'))


@app.route('/lianjia', methods=['GET'])
def lianjia():
    return render_template('lianjia.html')


@app.route('/api/district', methods=['GET'])
def district():
    districts = app.db_session.query(District).all()
    data = [{"id": d.id,
             "name": d.name} for d in districts]
    return jsonify(data=data)


@app.route('/api/area', methods=['GET'])
def area():
    district_id = request.args.get("district_id", "null")

    query = app.db_session.query(Area)
    if district_id != 'null':
        query = query.filter_by(district_id=district_id)

    data = [{"id": a.id,
             "name": a.name} for a in query.all()]
    return jsonify(data=data)


@app.route('/api/community', methods=['GET'])
def community():
    area_id = request.args.get("area_id", "null")

    query = app.db_session.query(CommunityLJ)
    if area_id != 'null':
        query = query.filter_by(area_id=area_id)

    data = [{"id": c.id,
             "name": c.name} for c in query.all()]
    return jsonify(data=data)


joined_table_1 = (
    HouseRecordLJ.__table__
    .join(HouseLJ.__table__,
          HouseRecordLJ.house_id == HouseLJ.id)
    .join(Community.__table__,
          HouseRecordLJ.community_id == Community.id)
    .join(District.__table__,
          Community.district_id == District.id)
    .join(Area.__table__,
          Community.area_id == Area.id)
    .join(BatchJob.__table__,
          (HouseRecordLJ.batch_type == BatchJob.type) & (
           HouseRecordLJ.batch_number == BatchJob.batch_number))
)


@app.route('/api/avg_price', methods=['GET'])
def avg_price():
    dt_end = datetime.now()
    dt_begin = dt_end - timedelta(hours=365*24*float(request.args["period"]))

    query = (select([HouseRecordLJ.community_id,
                     func.date(BatchJobLJ.created_at).label("created_at"),
                     (500/HouseLJ.area).label("area"),
                     (HouseRecordLJ.price/HouseLJ.area).label("price")])
             .select_from(joined_table_1)
             .where((HouseRecordLJ.price+HouseLJ.area).isnot(None))
             .where(BatchJobLJ.created_at.between(dt_begin, dt_end)))

    if request.args.get("community_id", None):
        query = query.where(Community.id == request.args["community_id"])
    elif request.args.get("area_id", None):
        query = query.where(Area.id == request.args["area_id"])
    elif request.args.get("district_id", None):
        query = query.where(District.id == request.args["district_id"])
    else:
        return my_jsonify(status="400", msg="请在区县、板块、小区中至少选择一个")

    df = pd.read_sql(query, app.db_engine,
                     index_col=["community_id", "created_at"])

    if df.empty:
        return jsonify(data={"x": [], "communities": []})

    # get map of community name
    query = (select([Community.id, Community.name])
             .where(Community.id.in_([int(i) for i in df.index.levels[0]])))
    c_name = {r.id: r.name for r in app.db_engine.execute(query).fetchall()}

    # calculate weighted average price
    s = df.groupby(level=[0, 1]).apply(w_avg)
    y_min = np.sort(s.values)[0] - 0.5

    new_index = pd.MultiIndex.from_product(
                    [s.index.levels[0], s.index.levels[1].sort_values()])
    s = s.reindex(new_index)
    s = s.groupby(level=0).fillna(method="ffill")
    s.fillna(0, inplace=True)

    x = s.index.levels[1].strftime('%Y-%m-%d').tolist()
    cs = [{"name": c_name[i],
           "id": int(i),
           "data": s.xs(i).values.tolist()}
          for i in s.index.levels[0]]

    return jsonify(data={"x": x,
                         "communities": cs,
                         "y_min": y_min})


def w_avg(df):
    return round(np.average(df["price"].values, weights=df["area"].values),2)

