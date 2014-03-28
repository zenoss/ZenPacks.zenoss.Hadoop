/*****************************************************************************
 *
 * Copyright (C) Zenoss, Inc. 2014, all rights reserved.
 *
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 *
 ****************************************************************************/

(function(){
var ZC = Ext.ns('Zenoss.component');
var ZD = Ext.ns('Zenoss.devices');

ZC.registerName('HadoopDataNode', _t('Hadoop Data Node'), _t('Hadoop Data Nodes'));
ZC.registerName('HadoopJobTracker', _t('Hadoop Job Tracker'), _t('Hadoop Job Trackers'));
ZC.registerName('HadoopSecondaryNameNode', _t('Hadoop Secondary Name Node'), _t('Hadoop Secondary Name Nodes'));

/* HadoopDataNode */
ZC.HadoopDataNodePanel = Ext.extend(ZC.ComponentGridPanel, {
    subComponentGridPanel: false,

    constructor: function(config) {
        config = Ext.applyIf(config||{}, {
            autoExpandColumn: 'name',
            componentType: 'HadoopDataNode',
            fields: [
                {name: 'uid'},
                {name: 'name'},
                {name: 'severity'},
                {name: 'status'},
                {name: 'usesMonitorAttribute'},
                {name: 'monitor'},
                {name: 'monitored'},
                {name: 'locking'},
                {name: 'last_contacted'},
                {name: 'health_state'},
            ],
            columns: [{
                id: 'severity',
                dataIndex: 'severity',
                header: _t('Events'),
                renderer: Zenoss.render.severity,
                width: 50
            },{
                id: 'name',
                dataIndex: 'name',
                header: _t('Name'),
            },{
                id: 'last_contacted',
                dataIndex: 'last_contacted',
                header: _t('Last Contacted'),
                width: 100
            },{
                id: 'health_state',
                dataIndex: 'health_state',
                header: _t('Health State'),
                width: 80
            },{
                id: 'status',
                dataIndex: 'status',
                header: _t('Status'),
                renderer: Zenoss.render.pingStatus,
                width: 60
            },{
                id: 'monitored',
                dataIndex: 'monitored',
                header: _t('Monitored'),
                renderer: Zenoss.render.checkbox,
                width: 60
            },{
                id: 'locking',
                dataIndex: 'locking',
                header: _t('Locking'),
                renderer: Zenoss.render.locking_icons,
                width: 60
            }]
        });
        ZC.HadoopDataNodePanel.superclass.constructor.call(this, config);
    }
});
Ext.reg('HadoopDataNodePanel', ZC.HadoopDataNodePanel);

/* HadoopJobTracker */
ZC.HadoopJobTrackerPanel = Ext.extend(ZC.ComponentGridPanel, {
    subComponentGridPanel: false,

    constructor: function(config) {
        config = Ext.applyIf(config||{}, {
            autoExpandColumn: 'name',
            componentType: 'HadoopJobTracker',
            fields: [
                {name: 'uid'},
                {name: 'name'},
                {name: 'status'},
                {name: 'severity'},
                {name: 'usesMonitorAttribute'},
                {name: 'monitor'},
                {name: 'monitored'},
                {name: 'locking'},
                {name: 'node_type'},
                {name: 'last_contacted'},
                {name: 'health_state'},
            ],
            columns: [{
                id: 'severity',
                dataIndex: 'severity',
                header: _t('Events'),
                renderer: Zenoss.render.severity,
                width: 50
            // },{
            //     id: 'node_type',
            //     dataIndex: 'node_type',
            //     header: _t('Node Type'),
            //     width: 150
            },{
                id: 'name',
                dataIndex: 'name',
                header: _t('Name'),
            },{
                id: 'last_contacted',
                dataIndex: 'last_contacted',
                header: _t('Last Contacted'),
                width: 100
            },{
                id: 'health_state',
                dataIndex: 'health_state',
                header: _t('Health State'),
                width: 80
            },{
                id: 'status',
                dataIndex: 'status',
                header: _t('Status'),
                renderer: Zenoss.render.pingStatus,
                width: 60
            },{
                id: 'monitored',
                dataIndex: 'monitored',
                header: _t('Monitored'),
                renderer: Zenoss.render.checkbox,
                width: 60
            },{
                id: 'locking',
                dataIndex: 'locking',
                header: _t('Locking'),
                renderer: Zenoss.render.locking_icons,
                width: 60
            }]
        });
        ZC.HadoopJobTrackerPanel.superclass.constructor.call(this, config);
    }
});
Ext.reg('HadoopJobTrackerPanel', ZC.HadoopJobTrackerPanel);

/* HadoopSecondaryNameNode */
ZC.HadoopSecondaryNameNodePanel = Ext.extend(ZC.ComponentGridPanel, {
    subComponentGridPanel: false,

    constructor: function(config) {
        config = Ext.applyIf(config||{}, {
            autoExpandColumn: 'name',
            componentType: 'HadoopSecondaryNameNode',
            fields: [
                {name: 'uid'},
                {name: 'name'},
                {name: 'status'},
                {name: 'severity'},
                {name: 'usesMonitorAttribute'},
                {name: 'monitor'},
                {name: 'monitored'},
                {name: 'locking'},
                {name: 'node_type'},
                {name: 'last_contacted'},
                {name: 'health_state'},
            ],
            columns: [{
                id: 'severity',
                dataIndex: 'severity',
                header: _t('Events'),
                renderer: Zenoss.render.severity,
                width: 50
            // },{
            //     id: 'node_type',
            //     dataIndex: 'node_type',
            //     header: _t('Node Type'),
            //     width: 150
            },{
                id: 'name',
                dataIndex: 'name',
                header: _t('Name'),
            },{
                id: 'last_contacted',
                dataIndex: 'last_contacted',
                header: _t('Last Contacted'),
                width: 100
            },{
                id: 'health_state',
                dataIndex: 'health_state',
                header: _t('Health State'),
                width: 80
            },{
                id: 'status',
                dataIndex: 'status',
                header: _t('Status'),
                renderer: Zenoss.render.pingStatus,
                width: 60
            },{
                id: 'monitored',
                dataIndex: 'monitored',
                header: _t('Monitored'),
                renderer: Zenoss.render.checkbox,
                width: 60
            },{
                id: 'locking',
                dataIndex: 'locking',
                header: _t('Locking'),
                renderer: Zenoss.render.locking_icons,
                width: 60
            }]
        });
        ZC.HadoopSecondaryNameNodePanel.superclass.constructor.call(this, config);
    }
});
Ext.reg('HadoopSecondaryNameNodePanel', ZC.HadoopSecondaryNameNodePanel);

})();
